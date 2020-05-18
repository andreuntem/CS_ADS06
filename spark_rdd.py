import datetime
import re


def count_elements_in_dataset(dataset):
    """
    Given a dataset loaded on Spark, return the
    number of elements.
    :param dataset: dataset loaded in Spark context
    :type dataset: a Spark RDD
    :return: number of elements in the RDD
    """
    return dataset.count()


def get_first_element(dataset):
    """
    Given a dataset loaded on Spark, return the
    first element
    :param dataset: dataset loaded in Spark context
    :type dataset: a Spark RDD
    :return: the first element of the RDD
    """
    return dataset.first()


def get_all_attributes(dataset):
    """
    Each element is a dictionary of attributes and their values for a post.
    Can you find the set of all attributes used throughout the RDD?
    The function dictionary.keys() gives you the list of attributes of a dictionary.
    :param dataset: dataset loaded in Spark context
    :type dataset: a Spark RDD
    :return: all unique attributes collected in a list
    """
    attribute = []
    for _, data in enumerate(dataset.collect()):
        for _, key in enumerate(data.keys()):
            attribute.append(key)
    return list(set(attribute))


def get_elements_w_same_attributes(dataset):
    """
    We see that there are more attributes than just the one used in the first element.
    This function should return all elements that have the same attributes
    as the first element.

    :param dataset: dataset loaded in Spark context
    :type dataset: a Spark RDD
    :return: an RDD containing only elements with same attributes as the
    first element
    """
    attributes = list(dataset.first().keys())
    return dataset.filter(lambda x: list(x.keys()) == attributes)


def get_min_max_timestamps(dataset):
    """
    Find the minimum and maximum timestamp in the dataset
    :param dataset: dataset loaded in Spark context
    :type dataset: a Spark RDD
    :return: min and max timestamp in a tuple object
    :rtype: tuple
    """
    min_str = dataset.map(lambda x: x['created_at']).reduce(lambda x, y: x if x < y else y)
    max_str = dataset.map(lambda x: x['created_at']).reduce(lambda x, y: x if x > y else y)
    min_date = datetime.datetime.strptime(min_str, '%Y-%m-%dT%H:%M:%SZ')
    max_date = datetime.datetime.strptime(max_str, '%Y-%m-%dT%H:%M:%SZ')
    return (min_date, max_date)


def get_number_of_posts_per_bucket(dataset, min_time, max_time):
    """
    Using the `get_bucket` function defined in the notebook (redefine it in this file), this function should return a
    new RDD that contains the number of elements that fall within each bucket.
    :param dataset: dataset loaded in Spark context
    :type dataset: a Spark RDD
    :param min_time: Minimum time to consider for buckets (datetime format)
    :param max_time: Maximum time to consider for buckets (datetime format)
    :return: an RDD with number of elements per bucket
    """
    def convert_str_to_datetime(x):
        return datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ')
    return dataset.filter(lambda x: min_time <= convert_str_to_datetime(x['created_at']) <= max_time)


def get_hour(x):
    x['hour'] = datetime.datetime.strptime(x['created_at'], '%Y-%m-%dT%H:%M:%SZ').hour
    return x


def get_number_of_posts_per_hour(dataset):
    """
    Using the `get_hour` function defined in the notebook (redefine it in this file), this function should return a
    new RDD that contains the number of elements per hour.
    :param dataset: dataset loaded in Spark context
    :type dataset: a Spark RDD
    :return: an RDD with number of elements per hour
    """
    dataset = dataset.map(lambda x: get_hour(x))
    hour_rdd = dataset.map(lambda x: (x['hour'], 1))
    out = hour_rdd.reduceByKey(lambda c1, c2: c1 + c2)
    return out


def get_score_per_hour(dataset):
    """
    The number of points scored by a post is under the attribute `points`.
    Use it to compute the average score received by submissions for each hour.
    :param dataset: dataset loaded in Spark context
    :type dataset: a Spark RDD
    :return: an RDD with average score per hour
    """
    dataset = dataset.map(lambda x: get_hour(x))
    points_hour = dataset.map(lambda x: (x['hour'], (x['points'], 1)))
    A = points_hour.reduceByKey(lambda first, second: (first[0]+second[0], first[1]+second[1]))
    out = A.map(lambda x: (x[0], x[1][0]/x[1][1]))
    return out


def get_proportion_of_scores(dataset):
    """
    It may be more useful to look at sucessful posts that get over 200 points.
    Find the proportion of posts that get above 200 points per hour.
    This will be the number of posts with points > 200 divided by the total number of posts at this hour.
    :param dataset: dataset loaded in Spark context
    :type dataset: a Spark RDD
    :return: an RDD with the proportion of scores over 200 per hour
    """
    dataset = dataset.map(lambda x: get_hour(x))
    dataset = dataset.map(lambda x: (x['hour'], (1, 1 if x['points'] > 200 else 0)))
    A = dataset.reduceByKey(lambda left, right: (left[0]+right[0], left[1]+right[1]))
    out = A.map(lambda x: (x[0], x[1][1]/x[1][0]))
    return out


def get_words(x):
    line = x['title']
    adj_title = re.compile('\w+').findall(line)
    x['len_title'] = len(adj_title)
    return x


def get_proportion_of_success(dataset):
    """
    Using the `get_words` function defined in the notebook to count the
    number of words in the title of each post, look at the proportion
    of successful posts for each title length.

    Note: If an entry in the dataset does not have a title, it should
    be counted as a length of 0.

    :param dataset: dataset loaded in Spark context
    :type dataset: a Spark RDD
    :return: an RDD with the proportion of successful post per title length
    """
    dataset = dataset.map(lambda x: get_words(x))
    A = dataset.map(lambda x: (x['len_title'], (1, 1 if x['points'] > 200 else 0)))
    B = A.reduceByKey(lambda left, right: (left[0]+right[0], left[1]+right[1]))
    out = B.map(lambda x: (x[0], x[1][1]/x[1][0]))
    return out


def get_title_length_distribution(dataset):
    """
    Count for each title length the number of submissions with that length.

    Note: If an entry in the dataset does not have a title, it should
    be counted as a length of 0.

    :param dataset: dataset loaded in Spark context
    :type dataset: a Spark RDD
    :return: an RDD with the number of submissions per title length
    """
    dataset = dataset.map(lambda x: get_words(x))
    A = dataset.map(lambda x: (x['len_title'], 1))
    out = A.reduceByKey(lambda x, y: x+y)
    return out
