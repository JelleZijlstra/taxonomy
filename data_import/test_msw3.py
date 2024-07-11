from data_import.msw3 import Token, TokenType, tokenize


def test_tokenize() -> None:
    text = "<i>Acanthonotus</i>  Goldfuss, 1809"
    assert list(tokenize(text)) == [
        Token(TokenType.OPEN_TAG, "<i>"),
        Token(TokenType.TEXT, "Acanthonotus"),
        Token(TokenType.CLOSE_TAG, "</i>"),
        Token(TokenType.TEXT, "Goldfuss"),
        Token(TokenType.COMMA, ","),
        Token(TokenType.DATE, "1809"),
    ]

    text = "<b><i>vitalinus</i></b> (Miranda-Ribeiro, 1936); <u>not allocated to subspecies:</u> <i>hemiurus</i> (Miranda-Ribeiro, 1936)."
    assert list(tokenize(text)) == [
        Token(TokenType.OPEN_TAG, "<b>"),
        Token(TokenType.OPEN_TAG, "<i>"),
        Token(TokenType.TEXT, "vitalinus"),
        Token(TokenType.CLOSE_TAG, "</i>"),
        Token(TokenType.CLOSE_TAG, "</b>"),
        Token(TokenType.OPEN_PAREN, "("),
        Token(TokenType.TEXT, "Miranda-Ribeiro"),
        Token(TokenType.COMMA, ","),
        Token(TokenType.DATE, "1936"),
        Token(TokenType.CLOSE_PAREN, ")"),
        Token(TokenType.SEMICOLON, ";"),
        Token(TokenType.OPEN_TAG, "<u>"),
        Token(TokenType.TEXT, "not allocated to subspecies:"),
        Token(TokenType.CLOSE_TAG, "</u>"),
        Token(TokenType.OPEN_TAG, "<i>"),
        Token(TokenType.TEXT, "hemiurus"),
        Token(TokenType.CLOSE_TAG, "</i>"),
        Token(TokenType.OPEN_PAREN, "("),
        Token(TokenType.TEXT, "Miranda-Ribeiro"),
        Token(TokenType.COMMA, ","),
        Token(TokenType.DATE, "1936"),
        Token(TokenType.CLOSE_PAREN, ")"),
        Token(TokenType.PERIOD, "."),
    ]
