import java.sql.*;

public class Main{

    private static final String PROTOCOL = "jdbc:postgresql://";        // URL-prefix
    private static final String DRIVER = "org.postgresql.Driver";       // Driver name
    private static final String URL_LOCALE_NAME = "localhost/";         // ваш компьютер + порт по умолчанию

    private static final String DATABASE_NAME = "vinil";          // FIXME имя базы

    public static final String DATABASE_URL = PROTOCOL + URL_LOCALE_NAME + DATABASE_NAME;
    public static final String USER_NAME = "postgres";                  // FIXME имя пользователя
    public static final String DATABASE_PASS = "postgres";              // FIXME пароль базы данных

    public static void main(String[] args) {

        // проверка возможности подключения
        checkDriver();
        checkDB();
        System.out.println("Подключение к базе данных | " + DATABASE_URL + "\n");

        // попытка открыть соединение с базой данных, которое java-закроет перед выходом из try-with-resources
        try (Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS)) {
            //TODO show all tables
            getRecords(connection);System.out.println();
            getPlayers(connection);System.out.println();
            getOrders(connection);System.out.println();
            getCustomers(connection);System.out.println();

            getCostofPlate(connection); System.out.println();
            getOrderbyID(connection); System.out.println();
            getCustomerbyID(connection); System.out.println();
            getTypeOfPlayerByID(connection);System.out.println();
            // TODO show with param
            getCustomersNamed(connection, "Игорь",false); System.out.println();// возьмем всех и найдем перебором
            getCustomersNamed(connection, "Алиса", true); System.out.println(); // тоже самое сделает БД
            getCustomersByOrders(connection, "Игорь"); System.out.println();
            getCustomersByTotal(connection,17000);System.out.println();
            // TODO correction
            addRecord(connection, 5, "The biltes",Date.valueOf("1999-12-02"),3,"Rock",2500); System.out.println();
            correctRecord(connection, 2, "Nikelback-Sunrise"); System.out.println();
            removeRecord(connection, 1); System.out.println();

        } catch (SQLException e) {
            // При открытии соединения, выполнении запросов могут возникать различные ошибки
            // Согласно стандарту SQL:2008 в ситуациях нарушения ограничений уникальности (в т.ч. дублирования данных) возникают ошибки соответствующие статусу (или дочерние ему): SQLState 23000 - Integrity Constraint Violation
            if (e.getSQLState().startsWith("23")){
                System.out.println("Произошло дублирование данных");
            } else throw new RuntimeException(e);
        }
    }

    // region // Проверка окружения и доступа к базе данных

    public static void checkDriver () {
        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println("Нет JDBC-драйвера! Подключите JDBC-драйвер к проекту согласно инструкции.");
            throw new RuntimeException(e);
        }
    }

    public static void checkDB () {
        try {
            Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS);
        } catch (SQLException e) {
            System.out.println("Нет базы данных! Проверьте имя базы, путь к базе или разверните локально резервную копию согласно инструкции");
            throw new RuntimeException(e);
        }
    }

    // endregion

    // region // SELECT-запросы без параметров в одной таблице
    private static void getRecords(Connection connection) throws SQLException{
        // имена столбцов
        String columnName0 = "id", columnName1 = "name", columnName2 = "release",columnName3 = "amount",columnName4 = "genre",columnName5 = "cost";
        // значения ячеек
        Date param2;
        int param0 = -1,param3=-1,param5=-1;
        String param1 = null,param4=null;

        Statement statement = connection.createStatement();     // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM catalog_records;"); // выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные, продвигаться по ним
            param5 = rs.getInt(columnName5); // значение ячейки, можно получить по имени; по умолчанию возвращается строка
            param4 = rs.getString(columnName4);
            param3 = rs.getInt(columnName3);
            param2 = rs.getDate(columnName2);
            param1 = rs.getString(columnName1);
            param0 = rs.getInt(columnName0);    // если точно уверены в типе данных ячейки, можно его сразу преобразовать
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | " + param4 + " | " + param5 );
        }
    }
    private static void getPlayers(Connection connection) throws SQLException{
        // имена столбцов
        String columnName0 = "id", columnName1 = "name", columnName2 = "characteristics",columnName3 = "type",columnName4 = "amount",columnName5 = "cost";
        // значения ячеек
        int param0 = -1,param5=-1;
        String param1 = null,param2=null,param3=null,param4=null;

        Statement statement = connection.createStatement();     // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM catalog_players;"); // выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные, продвигаться по ним
            param5 = rs.getInt(columnName5); // значение ячейки, можно получить по имени; по умолчанию возвращается строка
            param4 = rs.getString(columnName4);
            param3 = rs.getString(columnName3);
            param2 = rs.getString(columnName2);
            param1 = rs.getString(columnName1);
            param0 = rs.getInt(columnName0);    // если точно уверены в типе данных ячейки, можно его сразу преобразовать
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | " + param4 + " | " + param5 );
        }
    }
    private static void getOrders(Connection connection) throws SQLException{
        // имена столбцов
        String columnName0 = "id_order", columnName1 = "order", columnName2 = "date",columnName3 = "total",columnName4 = "status",columnName5="id_customer";
        // значения ячеек
        Date param2;
        int param0 = -1,param3=-1,param5;
        String param1 = null,param4=null;

        Statement statement = connection.createStatement();     // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM orders;"); // выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные, продвигаться по ним// значение ячейки, можно получить по имени; по умолчанию возвращается строка
            param5= rs.getInt(columnName5);
            param4 = rs.getString(columnName4);
            param3 = rs.getInt(columnName3);
            param2 = rs.getDate(columnName2);
            param1 = rs.getString(columnName1);
            param0 = rs.getInt(columnName0);    // если точно уверены в типе данных ячейки, можно его сразу преобразовать
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | " + param4 + " | " + param5 + " | " );
        }
    }
    private static void getCustomers(Connection connection) throws SQLException{
        // имена столбцов
        String columnName0 = "customer_id", columnName1 = "name", columnName2 = "town",columnName3 = "phone";
        // значения ячеек
        int param0 = -1,param3=-1;
        String param1 = null,param2=null;

        Statement statement = connection.createStatement();     // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM customers;"); // выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные, продвигаться по ним// значение ячейки, можно получить по имени; по умолчанию возвращается строка
            param3 = rs.getInt(columnName3);
            param2 = rs.getString(columnName2);
            param1 = rs.getString(columnName1);
            param0 = rs.getInt(columnName0);    // если точно уверены в типе данных ячейки, можно его сразу преобразовать
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | ");
        }
    }
    private static void getCostofPlate(Connection connection) throws SQLException{
        // имена столбцов
        String columnName0 = "id", columnName1 = "name", columnName2 = "cost";
        // значения ячеек
        int param0 = -1;
        String param1 = null, param2 = null;

        Statement statement = connection.createStatement();     // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM catalog_records;"); // выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные, продвигаться по ним
            param2 = rs.getString(columnName2); // значение ячейки, можно получить по имени; по умолчанию возвращается строка
            param1 = rs.getString(columnName1);
            param0 = rs.getInt(columnName0);    // если точно уверены в типе данных ячейки, можно его сразу преобразовать
            System.out.println(param0 + " | " + param1 + " | " + param2);
        }
    }
    static void getOrderbyID(Connection connection) throws SQLException {
        String columnName0 = "id_order", columnName1 = "order", columnName2 = "status";
        // значения ячеек
        int param0 = -1;
        String param1 = null,param2 = null;

        Statement statement = connection.createStatement();                 // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM orders;");  // выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные
            param0 = rs.getInt(columnName0); // значение ячейки, можно также получить по порядковому номеру (начиная с 1)
            param1 = rs.getString(columnName1);
            param2 = rs.getString(columnName2);
            System.out.println(param0 + " | " + param1 + " | " + param2);
        }
    }
    static void getCustomerbyID(Connection connection) throws SQLException {
        String columnName0 = "customer_id", columnName1 = "name";
        // значения ячеек
        int param0 = -1;
        String param1 = null;

        Statement statement = connection.createStatement();                 // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM customers;");  // выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные
            param0 = rs.getInt(columnName0); // значение ячейки, можно также получить по порядковому номеру (начиная с 1)
            param1 = rs.getString(columnName1);
            System.out.println(param0 + " | " + param1 + " | ");
        }
    }
    static void getTypeOfPlayerByID(Connection connection) throws SQLException {
        String columnName0 = "type", columnName1 = "id";
        // значения ячеек
        String param0 = null;
        int param1 = -1;

        Statement statement = connection.createStatement();                 // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM catalog_players;");  // выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные
            param0 = rs.getString(columnName0); // значение ячейки, можно также получить по порядковому номеру (начиная с 1)
            param1 = rs.getInt(columnName1);
            System.out.println(param0 + " | " + param1 + " | ");
        }
    }

    // endregion

    // region // SELECT-запросы с параметрами и объединением таблиц

    private static void getCustomersNamed(Connection connection, String name, boolean fromSQL) throws SQLException {
        if (name == null || name.isBlank()) return;// проверка "на дурака"

        if (fromSQL) {
            getCustomersNamed(connection, name);               // если флаг верен, то выполняем аналогичный запрос c условием (WHERE)
        } else {
            long time = System.currentTimeMillis();
            Statement statement = connection.createStatement();      // создаем оператор для простого запроса (без параметров)
            ResultSet rs = statement.executeQuery(
                    "SELECT customer_id, name, phone " +
                            "FROM customers");
            while (rs.next()) {  // пока есть данные перебираем их
                if (rs.getString(2).contains(name)) { // и выводим только определенный параметр
                    System.out.println(rs.getInt(1) + " | " + rs.getString(2) + " | " + rs.getInt(3));
                }
            }
            System.out.println("SELECT ALL and FIND (" + (System.currentTimeMillis() - time) + " мс.)");
        }
    }

    private static void getCustomersNamed(Connection connection, String name) throws SQLException {
        if (name == null || name.isBlank()) return; // проверка "на дурака"
        name = '%' + name + '%'; // переданное значение может быть дополнено сначала и в конце (часть слова)

        long time = System.currentTimeMillis();
        PreparedStatement statement = connection.prepareStatement(
                "SELECT customer_id, name, phone \n" +
                        "FROM customers\n" +
                        "WHERE name LIKE ? ;");  // создаем оператор шаблонного-запроса с "включаемыми" параметрами - ?
        statement.setString(1, name);           // "безопасное" добавление параметров в запрос; с учетом их типа и порядка (индексация с 1)
        ResultSet rs = statement.executeQuery();// выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные перебираем их и выводим
            System.out.println(rs.getInt(1) + " | " + rs.getString(2) + " | " + rs.getInt(3));
        }
        System.out.println("SELECT with WHERE (" + (System.currentTimeMillis() - time) + " мс.)");
    }

    private static void getCustomersByOrders(Connection connection, String name) throws SQLException {
        if (name == null || name.isBlank()) return;// проверка "на дурака"
        name = '%' + name + '%'; // переданное значение может быть дополнено сначала и в конце (часть слова)

        long time = System.currentTimeMillis();
        PreparedStatement statement = connection.prepareStatement(
                "SELECT customers.name,orders.order,orders.status,orders.total\n" +
                        "FROM orders\n" +
                        "JOIN customers ON orders.id_customer = customers.customer_id\n" +
                        "WHERE customers.name LIKE ?;");       // создаем оператор шаблонного-запроса с "включаемыми" параметрами - ?
        statement.setString(1, name);      // "безопасное" добавление параметров в запрос; с учетом их типа и порядка (индексация с 1)
        ResultSet rs = statement.executeQuery();    // выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные перебираем их и выводим
            System.out.println(rs.getString(1) + " | " + rs.getString(2) + " | " + rs.getString(3) + " | " + rs.getString(4));
        }
        System.out.println("SELECT with WHERE (" + (System.currentTimeMillis() - time) + " мс.)");
    }

    private static void getCustomersByTotal(Connection connection, int cost) throws SQLException {
        if (cost<-1) return;// проверка "на дурака"
        // переданное значение может быть дополнено сначала и в конце (часть слова)

        long time = System.currentTimeMillis();
        PreparedStatement statement = connection.prepareStatement(
                "SELECT customers.name,orders.order,orders.status,orders.total\n" +
                        "FROM orders\n" +
                        "JOIN customers ON orders.id_customer = customers.customer_id\n" +
                        "WHERE orders.total <= ?;");       // создаем оператор шаблонного-запроса с "включаемыми" параметрами - ?
        statement.setInt(1, cost);      // "безопасное" добавление параметров в запрос; с учетом их типа и порядка (индексация с 1)
        ResultSet rs = statement.executeQuery();    // выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные перебираем их и выводим
            System.out.println(rs.getString(1) + " | " + rs.getString(2) + " | " + rs.getString(3) + " | " + rs.getString(4));
        }
        System.out.println("SELECT with WHERE (" + (System.currentTimeMillis() - time) + " мс.)");
    }

    // endregion

    // region // CUD-запросы на добавление, изменение и удаление записей

    private static void addRecord (Connection connection, int id, String name, Date release_date, int amount, String genre, int cost)  throws SQLException {
        if (name == null || name.isBlank() || id < 0) return;

        PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO catalog_records(id, name,release,amount,genre,cost) VALUES (?, ?, ?, ?, ?, ?) returning id;", Statement.RETURN_GENERATED_KEYS);    // создаем оператор шаблонного-запроса с "включаемыми" параметрами - ?
        statement.setInt(1, id);    // "безопасное" добавление имени
        statement.setString(2, name);  // "безопасное" добавление количества глаз
        statement.setDate(3, release_date);
        statement.setInt(4, amount);
        statement.setString(5, genre);
        statement.setInt(6, cost);
        int count =
                statement.executeUpdate();  // выполняем запрос на коррекцию и возвращаем количество измененных строк

        ResultSet rs = statement.getGeneratedKeys(); // прочитать запрошенные данные от БД
        if (rs.next()) { // прокрутить к первой записи, если они есть
            System.out.println("Индетификатор пластинки " + rs.getInt(1));
        }

        System.out.println("INSERTed " + count + " records");
        getRecords(connection);
    }

    private static void correctRecord (Connection connection, int id, String name) throws SQLException {
        if (name == null || name.isBlank() || id < 0) return;

        PreparedStatement statement = connection.prepareStatement("UPDATE catalog_records SET name=? WHERE id=?;");
        statement.setString(1, name); // сначала что передаем
        statement.setInt(2, id);   // затем по чему ищем

        int count = statement.executeUpdate();  // выполняем запрос на коррекцию и возвращаем количество измененных строк

        System.out.println("UPDATEd " + count + " records");
        getRecords(connection);
    }

    private static void removeRecord(Connection connection,int id) throws SQLException {
        if (id<0) return;

        PreparedStatement statement = connection.prepareStatement("DELETE from catalog_records WHERE id=?;");
        statement.setInt(1, id);

        int count = statement.executeUpdate(); // выполняем запрос на удаление и возвращаем количество измененных строк
        System.out.println("DELETEd " + count + " records");
        getRecords(connection);
    }

    // endregion
}
