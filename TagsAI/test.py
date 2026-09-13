import os
import pandas as pd
import numpy as np
import transformers
from sklearn.model_selection import train_test_split
from sklearn.metrics import f1_score, precision_recall_fscore_support
import torch
from torch.nn import BCEWithLogitsLoss
from transformers import (
    AutoTokenizer,
    AutoModelForSequenceClassification,
    TrainingArguments,
    Trainer,
    DataCollatorWithPadding,
)
from datasets import Dataset, DatasetDict

MODEL_NAME = "DeepPavlov/rubert-base-cased"
OUTPUT_DIR = "./rubert_multilabel"

MAX_LEN = 256
BATCH_SIZE = 16
EPOCHS = 6
LR = 2e-5

RANDOM_SEED = 42

LABELS = []  # будет заполнен динамически из dataset.csv

THRESHOLD = 0.5

# ========== ПРИМЕР ДАННЫХ ==========
# Ожидаемый CSV: столбец 'text' и столбцы для тегов (0/1), например:
# text,ДТП,ЖКХ,Здравоохранение
# "Водитель въехал в столб",1,0,0
# Если у тебя теги в одном столбце через запятую — преобразуй в multi-hot перед использованием.


def load_data(csv_path):
    df = pd.read_csv(csv_path)
    # Определяем список меток: всё, кроме колонки 'text'
    label_cols = [c for c in df.columns if c != "text"]
    # Приводим колонки с метками к float
    for column_name in label_cols:
        df[column_name] = pd.to_numeric(df[column_name], errors="coerce").fillna(0).astype(float)
    df["labels"] = df[label_cols].values.tolist()
    df = df[["text", "labels"]]
    return df, label_cols

# --- prepare_datasets: простая токенизация и установка формата ---
def prepare_datasets(df, tokenizer):
    train_df, val_df = train_test_split(df, test_size=0.15, random_state=RANDOM_SEED, shuffle=True)

    # Проверка: убедимся, что в df метки float
    # (эта строка для отладки, можно убрать после подтверждения)
    print("sample label from dataframe:", train_df.loc[train_df.index[0], "labels"],
          "dtypes:", [type(x) for x in train_df.loc[train_df.index[0], "labels"]])

    ds_train = Dataset.from_pandas(train_df.reset_index(drop=True))
    ds_val = Dataset.from_pandas(val_df.reset_index(drop=True))
    ds = DatasetDict({"train": ds_train, "validation": ds_val})

    def tokenize_fn(example):
        return tokenizer(example["text"], truncation=True, max_length=MAX_LEN)

    ds = ds.map(tokenize_fn, batched=True)

    # Еще одна гарантия: привести labels к float32 внутри datasets (на всякий случай)
    def labels_to_float(batch):
        # batch["labels"] — список списков; приводим каждый элемент к float
        batch["labels"] = [ [float(x) for x in labs] for labs in batch["labels"] ]
        return batch

    ds = ds.map(labels_to_float, batched=True)

    # Debug: проверь тип и пример
    sample = ds["train"][0]
    print("sample from dataset (labels):", sample["labels"], " type:", type(sample["labels"][0]))

    # Теперь установить формат — HuggingFace превратит метки в torch tensors (float -> float32)
    ds.set_format(type="torch", columns=["input_ids", "attention_mask", "labels"])
    # Еще проверка: dtype тензора (для уверенности)
    t = ds["train"][0]["labels"]
    print("torch labels dtype after set_format:", t.dtype)  # должно показать torch.float32

    return ds


def compute_class_pos_weights(df, label_cols):
    # df has column 'labels' as list; compute per-label positives
    labels_matrix = np.array(df["labels"].tolist(), dtype=np.float32)
    num_samples = labels_matrix.shape[0]
    positives_per_label = labels_matrix.sum(axis=0)
    # pos_weight = (N - P) / P; protect from division by zero
    pos_weight = []
    for p in positives_per_label:
        if p <= 0:
            pos_weight.append(1.0)
        else:
            pos_weight.append(float((num_samples - p) / p))
    return torch.tensor(pos_weight, dtype=torch.float32)

def compute_metrics(pred):
    logits = pred.predictions
    labels = pred.label_ids
    probs = 1 / (1 + np.exp(-logits))
    preds = (probs >= THRESHOLD).astype(int)
    # micro F1
    f1_micro = f1_score(labels, preds, average="micro", zero_division=0)
    f1_macro = f1_score(labels, preds, average="macro", zero_division=0)
    precision, recall, _, _ = precision_recall_fscore_support(labels, preds, average="micro", zero_division=0)
    return {"f1_micro": f1_micro, "f1_macro": f1_macro, "precision": precision, "recall": recall}


class CustomTrainer(Trainer):
    def __init__(self, pos_weight: torch.Tensor | None = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pos_weight = pos_weight

    def compute_loss(self, model, inputs, return_outputs=False, num_items_in_batch=None, **kwargs):
        labels = inputs.get("labels")
        outputs = model(**{k: v for k, v in inputs.items() if k != "labels"})
        logits = outputs.get("logits")
        loss_fct = BCEWithLogitsLoss(pos_weight=self.pos_weight.to(logits.device) if self.pos_weight is not None else None)
        loss = loss_fct(logits, labels.float())
        return (loss, outputs) if return_outputs else loss

def main(csv_path):
    from transformers.trainer_callback import EarlyStoppingCallback

    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)

    df, label_cols = load_data(csv_path)
    # Сделаем LABELS глобальным на время исполнения, чтобы использовать в predict по умолчанию при желании
    global LABELS
    LABELS = list(label_cols)

    id2label = {i: name for i, name in enumerate(label_cols)}
    label2id = {name: i for i, name in enumerate(label_cols)}

    model = AutoModelForSequenceClassification.from_pretrained(
        MODEL_NAME,
        num_labels=len(label_cols),
        problem_type="multi_label_classification",
        id2label=id2label,
        label2id=label2id,
    )

    ds = prepare_datasets(df, tokenizer)

    data_collator = DataCollatorWithPadding(tokenizer=tokenizer)

    training_args = transformers.TrainingArguments(
        output_dir=OUTPUT_DIR,
        eval_strategy="epoch",
        save_strategy="epoch",
        per_device_train_batch_size=BATCH_SIZE,
        per_device_eval_batch_size=BATCH_SIZE,
        num_train_epochs=EPOCHS,
        learning_rate=LR,
        weight_decay=0.01,
        logging_steps=50,
        save_total_limit=2,
        load_best_model_at_end=True,
        metric_for_best_model="f1_micro",
        greater_is_better=True,
        fp16=torch.cuda.is_available(),
        warmup_ratio=0.1,
        gradient_accumulation_steps=1,
    )

    pos_weight = compute_class_pos_weights(df, label_cols)

    trainer = CustomTrainer(
        pos_weight=pos_weight,
        model=model,
        args=training_args,
        train_dataset=ds["train"],
        eval_dataset=ds["validation"],
        tokenizer=tokenizer,
        data_collator=data_collator,
        compute_metrics=compute_metrics,
        callbacks=[EarlyStoppingCallback(early_stopping_patience=2, early_stopping_threshold=1e-4)],
    )

    trainer.train()
    # сохранить модель и tokenizer
    trainer.save_model(OUTPUT_DIR)
    tokenizer.save_pretrained(OUTPUT_DIR)

    print("Обучение завершено. Модель сохранена в", OUTPUT_DIR)

    return model, tokenizer, LABELS

def predict(texts, model, tokenizer, labels=None, threshold=THRESHOLD):
    model.eval()
    encoded = tokenizer(texts, truncation=True, padding=True, return_tensors="pt", max_length=MAX_LEN)
    with torch.no_grad():
        out = model(**{k:v.to(model.device) for k,v in encoded.items()})
        logits = out.logits.cpu().numpy()
    probs = 1 / (1 + np.exp(-logits))
    preds = (probs >= threshold).astype(int)
    results = []
    active_labels = labels if labels is not None else LABELS
    for p, prob_row in zip(preds, probs):
        tags = [active_labels[i] for i, v in enumerate(p) if v == 1]
        probs_for_tags = {active_labels[i]: float(prob_row[i]) for i in range(len(active_labels))}
        results.append({"tags": tags, "probs": probs_for_tags})
    return results

if __name__ == "__main__":
    csv_path = os.path.join(os.getcwd(), "dataset.csv")
    model, tokenizer, labels = main(csv_path)

    texts = [
        "Президент РФ выступил с посланием",
        "В поликлинике открыли новый кабинет",
        "На трассе произошло ДТП с автобусом",
    ]
    preds = predict(texts, model, tokenizer, labels=labels)
    for t, p in zip(texts, preds):
        print("Текст:", t)
        print("Предсказанные теги:", p["tags"])
        print("Вероятности:", p["probs"])
        print("-"*40)
