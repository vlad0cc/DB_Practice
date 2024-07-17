import java.sql.*;
import java.util.Scanner;

public class JDBCRunnerMusicService {
    private static final String PROTOCOL = "jdbc:postgresql://";
    private static final String URL_LOCALE_NAME = "localhost/";
    private static final String DATABASE_NAME = "MusicServiceDB";
    public static final String USER_NAME = "postgres";
    public static final String DATABASE_PASS = "postgres";
    public static final String DATABASE_URL = PROTOCOL + URL_LOCALE_NAME + DATABASE_NAME;

    public static void main(String[] args) {
        try (Connection conn = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS)) {
            checkDriver();
            checkDB();

             displayAllArtists(conn);
             System.out.println();
             addArtist(conn, "Putin", 30);
             addAlbum(conn, "New Album", 16, "2024-07-17", "Pop", 0);
             displayMostPopularAlbum(conn);
             displayAlbumsByArtist(conn, 1);
             displayYoungerArtists(conn, 30);
             displaySongsByArtistAndAlbum(conn, 1, 1, 300);
             displayPopularGenresByArtist(conn, 1);
             deleteAlbum(conn, "New Album");

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public static void checkDriver() {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            System.out.println("JDBC Driver org.postgresql.Driver отсутствует! Подключите драйвер к проекту.");
            throw new RuntimeException(e);
        }
    }
    public static void checkDB() {
        try (Connection conn = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS)) {
            System.out.println("Успешное подключение к базе данных " + DATABASE_NAME);
        } catch (SQLException e) {
            System.out.println("Ошибка подключения к базе данных " + DATABASE_NAME);
            throw new RuntimeException(e);
        }
    }
    public static void displayAllArtists(Connection conn) throws SQLException {
        String sql = "SELECT * FROM Artists";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                int id = rs.getInt("id");
                String artist = rs.getString("artist");
                int artistAge = rs.getInt("artist_age");
                System.out.println("ID: " + id + ", Artist: " + artist + ", Age: " + artistAge);
            }
        }
    }
    public static void displayAlbumsByArtist(Connection conn, int artistId) throws SQLException {
        String sql = "SELECT * FROM Albums WHERE id_artist = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, artistId);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                int id = rs.getInt("id");
                String name = rs.getString("name");
                String date = rs.getString("date");
                String genre = rs.getString("genre");
                long num_of_listens = rs.getLong("num_of_listens");
                System.out.println("ID: " + id + ", Name: " + name + ", Date: " + date + ", Genre: " + genre + ", Listens: " + num_of_listens);
            }
        }
    }
    public static void displayMostPopularAlbum(Connection conn) throws SQLException {
        String sql = "SELECT * FROM Albums ORDER BY num_of_listens DESC LIMIT 1";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                int id = rs.getInt("id");
                String name = rs.getString("name");
                String date = rs.getString("date");
                String genre = rs.getString("genre");
                long num_of_listens = rs.getLong("num_of_listens");
                System.out.println("Most Popular Album:");
                System.out.println("ID: " + id + ", Name: " + name + ", Date: " + date + ", Genre: " + genre + ", Listens: " + num_of_listens);
            }
        }
    }
    public static void addArtist(Connection conn, String artistName, int artistAge) throws SQLException {
        String sql = "INSERT INTO Artists (artist, artist_age) VALUES (?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, artistName);
            pstmt.setInt(2, artistAge);
            int affectedRows = pstmt.executeUpdate();
            System.out.println("Добавлено нового артиста: " + artistName);
        }
    }
    public static void addAlbum(Connection conn, String albumName, int artistId, String date, String genre, int numListens) throws SQLException {
        String sql = "INSERT INTO Albums (id_artist, name, date, genre, num_of_listens) VALUES (?, ?, ?, ?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, artistId);
            pstmt.setString(2, albumName);
            pstmt.setDate(3, Date.valueOf(date));
            pstmt.setString(4, genre);
            pstmt.setInt(5, numListens);
            int affectedRows = pstmt.executeUpdate();
            System.out.println("Добавлен новый альбом: " + albumName);
        }
    }
    public static void displayMostPopularAlbumByArtist(Connection conn, int artistId) throws SQLException {
        String sql = "SELECT * FROM Albums WHERE id_artist = ? ORDER BY num_of_listens DESC LIMIT 1";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, artistId);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                int id = rs.getInt("id");
                String name = rs.getString("name");
                String date = rs.getString("date");
                String genre = rs.getString("genre");
                long num_of_listens = rs.getLong("num_of_listens");
                System.out.println("Most Popular Album for Artist " + artistId + ":");
                System.out.println("ID: " + id + ", Name: " + name + ", Date: " + date + ", Genre: " + genre + ", Listens: " + num_of_listens);
            }
        }
    }
    public static void displayYoungerArtists(Connection conn, int age) throws SQLException {
        String sql = "SELECT * FROM Artists WHERE artist_age < ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, age);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                int id = rs.getInt("id");
                String artist = rs.getString("artist");
                int artistAge = rs.getInt("artist_age");
                System.out.println("ID: " + id + ", Artist: " + artist + ", Age: " + artistAge);
            }
        }
    }
    public static void displaySongsByArtistAndAlbum(Connection conn, int artistId, int albumId, int duration) throws SQLException {
        String sql = "SELECT * FROM Songs WHERE id_album = ? AND duration <= ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, albumId);
            pstmt.setInt(2, duration);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                int id = rs.getInt("id");
                String name = rs.getString("name");
                int songDuration = rs.getInt("duration");
                System.out.println("ID: " + id + ", Name: " + name + ", Duration: " + songDuration);
            }
        }
    }
    public static void displayPopularGenresByArtist(Connection conn, int artistId) throws SQLException {
        String sql = "SELECT genre, COUNT(*) AS num_albums FROM Albums WHERE id_artist = ? GROUP BY genre ORDER BY num_albums DESC";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, artistId);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                String genre = rs.getString("genre");
                int numAlbums = rs.getInt("num_albums");
                System.out.println("Genre: " + genre + ", Number of Albums: " + numAlbums);
            }
        }
    }
    public static void deleteAlbum(Connection conn, String albumName) throws SQLException {
        String sql = "DELETE FROM Albums WHERE name = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, albumName);
            int affectedRows = pstmt.executeUpdate();
            System.out.println("Удалено альбомов: " + affectedRows);
        }
    }
}