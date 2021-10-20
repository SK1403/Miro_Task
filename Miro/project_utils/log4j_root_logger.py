class Log4jRootLogger(object):
    """
        Wrapper class for Log4j root logger
        :param spark SparkSession: SparkSession object
    """

    def __init__(self, spark):
        # capture job details with prefix
        conf = spark.sparkContext.getConf()
        app_id = conf.get('spark.app.id')
        app_name = conf.get('spark.app.name')

        log4jLogger = spark._jvm.org.apache.log4j
        message_prefix = '[' + app_name + ' : ' + app_id + ']'
        self.logger = log4jLogger.LogManager.getLogger(message_prefix)

    def error(self, message):
        """
        Explanation: Log ERROR with message
        :param message str: Error message
        :return: None
        """
        self.logger.error(message)
        return None

    def warn(self, message):
        """
        Explanation: Log WARN with message
        :param message str: Warning message
        :return: None
        """
        self.logger.warn(message)
        return None

    def debug(self, message):
        """
        Explanation: Log DEBUG with message
        :param message str: Debugging message
        :return: None
        """
        self.logger.debug(message)
        return None

    def info(self, message):
        """
        Explanation: Log INFO with message
        :param message str: Information message
        :return: None
        """
        self.logger.info(message)
        return None
