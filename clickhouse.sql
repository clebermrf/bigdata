CREATE DATABASE vendor;

CREATE TABLE vendor.authors (
    name String,
    birth_date String,
    died_at Nullable(String),
    nationality String
)
ENGINE = HDFS('hdfs://namenode:9000/user/vendor/authors/*', 'Parquet');

CREATE TABLE vendor.reviews (
    movie String,
    rate UInt16,
    book String,
    resume String
)
ENGINE = HDFS('hdfs://namenode:9000/user/vendor/reviews/*', 'Parquet');

CREATE TABLE vendor.books (
    name String,
    pages UInt16,
    author String,
    publisher String
)
ENGINE = HDFS('hdfs://namenode:9000/user/vendor/books/*', 'Parquet');

CREATE DATABASE internal;
CREATE TABLE internal.movies (
    title String,
    duration_mins UInt16,
    original_language String,
    size_mb UInt16
)
ENGINE = HDFS('hdfs://namenode:9000/internal/movies/*', 'CSVWithNames');

CREATE TABLE internal.streams (
    movie_title String,
    user_email String,
    size_mb Float32,
    start_at String,
    end_at String
)
ENGINE = HDFS('hdfs://namenode:9000/internal/streams/*', 'CSVWithNames');

CREATE TABLE internal.users (
    first_name String,
    last_name String,
    email String
)
ENGINE = HDFS('hdfs://namenode:9000/internal/users/*', 'CSVWithNames');

