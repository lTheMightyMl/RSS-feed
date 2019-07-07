package in.nimbo.exception;


class JimboException extends Exception {

    JimboException(String message) {
        super("JimboException-" + message);
    }

}
