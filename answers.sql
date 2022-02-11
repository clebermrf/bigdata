-- What percentage of the streamed movies are based on books?

SELECT avg(movie_title IN (
    SELECT movie
    FROM vendor.reviews
))
FROM (
    SELECT DISTINCT movie_title
    FROM internal.streams
)

Answer: 0.9340


-- How many users were watching "Unforgiven" on Christmas morning (between 7 am and 12 noon on December 25)?

SELECT countDistinct(user_email) as uniqQnt,
    count() AS qnt

FROM internal.streams
WHERE movie_title = 'Unforgiven'
AND (
    parseDateTimeBestEffortOrZero(start_at)
    BETWEEN toDateTime('2021-12-25 07:00:00') AND toDateTime('2021-12-25 12:00:00')

    OR parseDateTimeBestEffortOrZero(end_at)
    BETWEEN toDateTime('2021-12-25 07:00:00') AND toDateTime('2021-12-25 12:00:00')
)

Answer: 5


-- How many movies based on books written by Singaporeans authors were streamed that month?

SELECT countDistinct(movie_title), count()
FROM internal.streams
WHERE movie_title IN (

    SELECT movie
    FROM vendor.reviews
    WHERE book IN (

        SELECT name
        FROM vendor.books
        WHERE author IN (

            SELECT name
            FROM vendor.authors
            WHERE nationality = 'Singaporeans'
        )
    )
)

Answer: 3 movies


-- What's the average streaming duration?

SELECT avg(
    dateDiff(
        'minute',
        parseDateTimeBestEffortOrZero(start_at),
        parseDateTimeBestEffortOrZero(end_at)
    ) AS diff),
    quantile(0.50)(diff)

FROM internal.streams

Answer: 722.26 minutes


-- What's the median streaming size in gigabytes?

SELECT avg(size_mb * 0.001),
    quantile(0.50)(size_mb * 0.001)

FROM internal.streams

Answer: 0.9319 GB


-- How many users watched at least 50% of any movie in the last week of the month (7 days)?

SELECT countIf(dateDiff('minute', start, end) / duration_mins > 0.5)
FROM (

    SELECT movie_title,
        if(
            parseDateTimeBestEffortOrZero(start_at) < toDateTime('2021-12-25 00:00:00'),
            toDateTime('2021-12-25 00:00:00'), parseDateTimeBestEffortOrZero(start_at)
        ) AS start,
        if(
            parseDateTimeBestEffortOrZero(end_at) > toDateTime('2021-12-31 23:59:59'),
            toDateTime('2021-12-31 23:59:59'), parseDateTimeBestEffortOrZero(end_at)
        ) AS end

    FROM internal.streams

    WHERE toDate(parseDateTimeBestEffortOrZero(start_at))
    BETWEEN toDate('2021-12-25') AND toDate('2021-12-31')

    OR toDate(parseDateTimeBestEffortOrZero(end_at))
    BETWEEN toDate('2021-12-25') AND toDate('2021-12-31')

) AS T

ANY LEFT JOIN (
    SELECT title, duration_mins
    FROM internal.movies
) AS M
ON T.movie_title = M.title

Answer: 2006
