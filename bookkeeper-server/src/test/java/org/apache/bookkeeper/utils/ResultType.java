package org.apache.bookkeeper.utils;

public enum ResultType {
    ILLEGAL_ARG_ERR, // IllegalArgumentException
    UNEXPECTED_ERR, // UnexpectedConditionException
    OK,
    NULL_PTR_ERR
}
