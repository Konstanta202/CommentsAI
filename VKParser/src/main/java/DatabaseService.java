import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Optional;

public class DatabaseService {
    private final String url;
    private final String user;
    private final String password;
    
    public DatabaseService() {
        this.url = System.getenv("DB_URL") != null ? System.getenv("DB_URL") : "jdbc:postgresql://vkservice-db:5432/vkservice_db";
        this.user = System.getenv("DB_USER") != null ? System.getenv("DB_USER") : "postgres";
        this.password = System.getenv("DB_PASSWORD") != null ? System.getenv("DB_PASSWORD") : "1111";
        
        System.out.println("DatabaseService initialized with URL: " + url);
    }
    
    public void saveGroup(Long groupId, String name, Long peopleCount) {
        String sql = """
            INSERT INTO groups (group_id, name, people_count, created_at, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (group_id) 
            DO UPDATE SET 
                name = EXCLUDED.name,
                people_count = EXCLUDED.people_count,
                updated_at = CURRENT_TIMESTAMP
            """;
            
        executeUpdate(sql, groupId, name, peopleCount);
        System.out.println("Сохранена/обновлена группа: " + groupId);
    }
    
    public void savePost(Long groupId, Long postId, Long commCount, Timestamp createdAt) {
        String sql = """
            INSERT INTO posts (group_id, post_id, comm_count, created_at, updated_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT (group_id, post_id) 
            DO UPDATE SET 
                comm_count = EXCLUDED.comm_count,
                updated_at = CURRENT_TIMESTAMP
            """;
            
        executeUpdate(sql, groupId, postId, commCount, createdAt);
        System.out.println("Сохранен/обновлен пост: " + postId + " для группы: " + groupId);
    }
    
    public void saveComment(Long postsId, Long userId, String comment, Timestamp createdAt) {
        if (userId == null) {
            System.err.println("ОШИБКА: userId не может быть null");
            return;
        }

        if (userId > 0) {
            // Проверяем существование пользователя для положительных ID
            if (!findUserById(userId)) {
                System.err.println("ОШИБКА: Пользователь с ID " + userId + " не существует");
                return;
            }
        }

        String sql = """
            INSERT INTO comments (posts_id, user_id, comment, created_at)
            VALUES (?, ?, ?, ?)
            """;
            
        executeUpdate(sql, postsId, userId, comment, createdAt);
        System.out.println("Сохранен комментарий для posts_id: " + postsId);
    }
    
        public void saveUserToDatabase(Long userId, String firstName, String lastName, Boolean isClosed, 
                              String country, String city, String phoneNumber, String about) {
        String sql = """
            INSERT INTO users (user_id, first_name, last_name, is_closed, country, city, phone_number, about)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (user_id) 
            DO UPDATE SET 
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                is_closed = EXCLUDED.is_closed,
                country = EXCLUDED.country,
                city = EXCLUDED.city,
                phone_number = EXCLUDED.phone_number,
                about = EXCLUDED.about,
                updated_at = CURRENT_TIMESTAMP
            """;
        
        executeUpdate(sql, userId, firstName, lastName, isClosed, country, city, phoneNumber, about);
        System.out.println("Сохранен/обновлен пользователь: " + userId + " - " + firstName + " " + lastName);
    }

    public boolean userExists(Long userId) {
        String sql = "SELECT COUNT(*) FROM users WHERE user_id = ?";
        
        try (Connection conn = getConnection();
            PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setLong(1, userId);
            
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    int count = rs.getInt(1);
                    return count > 0;
                }
            }
            
        } catch (SQLException e) {
            System.err.println("Ошибка проверки пользователя " + userId + ": " + e.getMessage());
            e.printStackTrace();
        }
        
        return false;
    }

        public boolean findUserById(Long userId) {
        String sql = "SELECT COUNT(*) FROM users WHERE id = ?";
        
        try (Connection conn = getConnection();
            PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setLong(1, userId);
            
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    int count = rs.getInt(1);
                    return count > 0;
                }
            }
            
        } catch (SQLException e) {
            System.err.println("Ошибка проверки пользователя " + userId + ": " + e.getMessage());
            e.printStackTrace();
        }
        
        return false;
    }

    public Optional<Long> findUserIdByVkId(Long vkUserId) {
        String sql = "SELECT id FROM users WHERE user_id = ?";
        
        try (Connection conn = getConnection();
            PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setLong(1, vkUserId);
            
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(rs.getLong("id")); // Используем getLong для BIGSERIAL
                }
            }
            
        } catch (SQLException e) {
            System.err.println("Ошибка поиска пользователя " + vkUserId + ": " + e.getMessage());
        }
        
        return Optional.empty(); // Четко указываем что пользователь не найден
    }

    public Optional<Long> findPostsIdByPostAndGroupId(Long postId, Long groupId) {
        String sql = "SELECT id FROM posts WHERE post_id = ? AND group_id = ?";
        
        try (Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setLong(1, postId);
            pstmt.setLong(2, groupId);
            ResultSet rs = pstmt.executeQuery();
            
            if (rs.next()) {
                return Optional.of(rs.getLong("id"));
            }
        } catch (SQLException e) {
            System.err.println("Ошибка поиска posts_id: " + e.getMessage());
        }
        
        return Optional.empty();
    }
    
    private void executeUpdate(String sql, Object... params) {
        try (Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            for (int i = 0; i < params.length; i++) {
                pstmt.setObject(i + 1, params[i]);
            }
            
            int affectedRows = pstmt.executeUpdate();
            System.out.println("Затронуто строк: " + affectedRows);
        } catch (SQLException e) {
            System.err.println("Ошибка выполнения SQL: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }
}