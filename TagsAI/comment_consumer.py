import os
import json
import time
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional

import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification


LOGGER = logging.getLogger("comment_consumer")


def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def _find_model_dir(preferred: Optional[str]) -> Path:
    candidates: List[Path] = []

    # 1) If env or arg provided
    if preferred:
        p = Path(preferred)
        candidates.append(p)
        # also try absolute from CWD
        candidates.append(Path.cwd() / p)

    # 2) Relative to script dir
    here = Path(__file__).resolve().parent
    candidates.extend([
        here / "rubert_multilabel",
        here.parent / "rubert_multilabel",
        Path.cwd() / "rubert_multilabel",
    ])

    checked = []
    for c in candidates:
        try:
            c = c.resolve()
        except Exception:
            pass
        checked.append(str(c))
        if c.exists() and c.is_dir() and (c / "config.json").exists():
            return c

    raise SystemExit(
        "Model directory not found. Provide MODEL_DIR env var to a folder containing config.json.\n"
        + "Tried: " + ", ".join(checked)
    )


def load_model(model_dir: Optional[str]):
    resolved_dir = _find_model_dir(model_dir)
    LOGGER.info("Loading model from %s", resolved_dir)
    tokenizer = AutoTokenizer.from_pretrained(str(resolved_dir))
    model = AutoModelForSequenceClassification.from_pretrained(str(resolved_dir))
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model.to(device)
    model.eval()
    return model, tokenizer, device, resolved_dir


def load_labels_from_model_config(model_dir: Path) -> List[str]:
    """Загружает теги из конфигурации модели"""
    config_path = model_dir / "config.json"
    
    if not config_path.exists():
        LOGGER.warning("Config file not found at %s, using fallback labels", config_path)
        return load_fallback_labels()
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        # Проверяем наличие id2label в конфигурации
        if "id2label" in config:
            id2label = config["id2label"]
            # Сортируем по ID для правильного порядка
            labels = [id2label[str(i)] for i in range(len(id2label))]
            LOGGER.info("Loaded %d labels from model config: %s", len(labels), labels)
            return labels
        else:
            LOGGER.warning("No id2label found in config, using fallback labels")
            return load_fallback_labels()
            
    except Exception as e:
        LOGGER.error("Failed to load labels from config: %s, using fallback", e)
        return load_fallback_labels()


def load_fallback_labels() -> List[str]:
    """Fallback теги если не удалось загрузить из конфигурации"""
    LOGGER.warning("Using fallback labels - this may not match your model!")
    return ["ДТП", "ЖКХ", "Здравоохранение"]


def sigmoid(x):
    return 1 / (1 + torch.exp(-x))


def predict_texts(texts: List[str], model, tokenizer, device, model_path: Path, threshold: float = 0.8) -> List[Dict[str, Any]]:
    enc = tokenizer(texts, truncation=True, padding=True, return_tensors="pt", max_length=128)
    enc = {k: v.to(device) for k, v in enc.items()}
    with torch.no_grad():
        out = model(**enc)
        probs = sigmoid(out.logits).cpu().numpy()
    preds = (probs >= threshold).astype(int)
    
    # Загружаем теги из конфигурации модели
    labels = load_labels_from_model_config(model_path)
    
    # Проверяем соответствие количества тегов
    if len(labels) != probs.shape[1]:
        LOGGER.error("Mismatch: model outputs %d probabilities but config has %d labels", 
                    probs.shape[1], len(labels))
        LOGGER.error("Model path: %s", model_path)
        LOGGER.error("Labels: %s", labels)
        raise ValueError(f"Model output dimension ({probs.shape[1]}) doesn't match labels count ({len(labels)})")
    
    results: List[Dict[str, Any]] = []
    for row_pred, row_prob in zip(preds, probs):
        tags = [labels[i] for i, v in enumerate(row_pred) if v == 1]
        results.append({
            "tags": tags,
            "probs": {labels[i]: float(row_prob[i]) for i in range(len(labels))}
        })
    return results


def main():
    setup_logging(os.getenv("LOG_LEVEL", "INFO"))

    model_dir = os.getenv("MODEL_DIR", None)
    topic = os.getenv("KAFKA_TOPIC", "vk_comments")
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    group_id = os.getenv("KAFKA_GROUP", "vk_comment_consumer")
    batch_size = int(os.getenv("BATCH_SIZE", "16"))
    threshold = float(os.getenv("THRESHOLD", "0.5"))
    min_tags = int(os.getenv("MIN_TAGS", "1"))

    model, tokenizer, device, model_path = load_model(model_dir)

    try:
        from kafka import KafkaConsumer, KafkaProducer  # type: ignore
    except Exception as e:
        raise SystemExit(f"Install kafka-python to use this consumer: {e}")

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        group_id=group_id,
        enable_auto_commit=True,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda v: v.decode("utf-8") if v is not None else None,
        consumer_timeout_ms=10000,
        max_poll_records=batch_size,
    )

    # Optional: produce results to another topic
    result_topic = os.getenv("KAFKA_RESULT_TOPIC")
    producer = None
    
    # Создаем producer если заданы топики для отправки
    if result_topic :
        producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            key_serializer=lambda v: str(v).encode("utf-8") if v is not None else None,
            linger_ms=50,
            acks=1,
            compression_type="gzip",
        )
        LOGGER.info("Kafka producer created for topics: result=%s, text_tags=%s", result_topic)
    else:
        LOGGER.warning("No Kafka topics configured for output. Set KAFKA_RESULT_TOPIC or KAFKA_TEXT_TAGS_TOPIC to send data to Kafka")

    # Загружаем и логируем информацию о тегах
    labels = load_labels_from_model_config(model_path)
    LOGGER.info("Model loaded with %d labels: %s", len(labels), labels)
    
    LOGGER.info("Started consumer. topic=%s bootstrap=%s group=%s threshold=%.2f min_tags=%d", 
                topic, bootstrap, group_id, threshold, min_tags)
    buffer: List[Dict[str, Any]] = []
    last_infer_ts = time.time()
    try:
        while True:
            for msg in consumer.poll(timeout_ms=2000, max_records=batch_size).values():
                for record in msg:
                    buffer.append(record.value)
            if buffer and (len(buffer) >= batch_size or (time.time() - last_infer_ts) > 2.0):
                # Filter out records with null/empty text
                valid_records = []
                skipped = 0
                for x in buffer:
                    raw_text = x.get("text") or x.get("message")
                    if raw_text is None:
                        LOGGER.debug("Skipping record - no text/message field: %s", x.get("id", "unknown"))
                        skipped += 1
                        continue
                    text = str(raw_text).strip()
                    if not text:
                        LOGGER.debug("Skipping record - empty text: %s", x.get("id", "unknown"))
                        skipped += 1
                        continue
                    valid_records.append((x, text))

                if skipped:
                    LOGGER.info("Skipped %d records with null/empty text", skipped)

                if not valid_records:
                    buffer.clear()
                    last_infer_ts = time.time()
                    continue

                texts = [t for (_, t) in valid_records]
                ids = [
                    (rec.get("_owner_id"), rec.get("_post_id"), rec.get("id"))
                    for (rec, _) in valid_records
                ]
                
                LOGGER.info("Processing %d valid records for ML prediction", len(valid_records))
                preds = predict_texts(texts, model, tokenizer, device, model_path, threshold=threshold)
                
                # Счетчики для статистики
                processed = 0
                skipped_no_tags = 0
                for ((owner_id, post_id, cid), pred, (original, text)) in zip(ids, preds, valid_records):
                    # Проверяем, есть ли теги в предсказании
                    tags = pred.get("tags", [])
                    if len(tags) < min_tags:
                        LOGGER.debug("Skipping comment %s_%s_%s - only %d tags predicted (need %d)", 
                                   owner_id, post_id, cid, len(tags), min_tags)
                        skipped_no_tags += 1
                        continue
                        
                    result = {
                        "owner_id": owner_id,
                        "post_id": post_id,
                        "comment_id": cid,
                        "text": text,
                        **pred,
                    }
                    LOGGER.info("Predicted tags=%s for comment %s_%s_%s", result.get("tags"), owner_id, post_id, cid)
                    processed += 1
                    
                    if producer:
                        key = f"{owner_id}_{post_id}_{cid}"
                        try:
                            if result_topic:
                                future = producer.send(result_topic, key=key, value=result)
                                LOGGER.debug("Sent to %s: %s", result_topic, key)
                        except Exception as e:
                            LOGGER.error("Failed to send message to Kafka: %s", e)
                    else:
                        LOGGER.warning("No Kafka producer available - cannot send message to Kafka")
                
                # Логируем статистику обработки
                if skipped_no_tags > 0:
                    LOGGER.info("Batch processed: %d sent to Kafka, %d skipped (< %d tags)", 
                               processed, skipped_no_tags, min_tags)
                else:
                    LOGGER.info("Batch processed: %d sent to Kafka", processed)
                    
                buffer.clear()
                last_infer_ts = time.time()
    except KeyboardInterrupt:
        LOGGER.info("Shutting down...")
    finally:
        consumer.close()
        if producer:
            producer.flush(5)
            producer.close()


if __name__ == "__main__":
    main()


