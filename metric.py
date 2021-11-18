import time


class MetricCollection(object):

    def __init__(self):
        self.metrics = []

    def append(self, metric):
        self.metrics.append(metric)

    def clear(self):
        self.metrics = []

    def __str__(self):
        return "\n".join(str(m) for m in self.metrics)


class Metric(object):

    def __init__(self, measurement):
        self.measurement = measurement
        self.values = {}
        self.tags = dict()
        self.timestamp = None

    def set_values(self, data: dict):
        self.values = data

    def with_timestamp(self, timestamp):
        self.timestamp = timestamp

    def add_tag(self, name, value):
        self.tags[str(name)] = str(value)

    def add_value(self, name, value):
        self.values[str(name)] = value

    def __str__(self):
        # Escape measurement manually
        escaped_measurement = self.measurement.replace(',', '\\,')
        escaped_measurement = escaped_measurement.replace(' ', '\\ ')
        protocol = escaped_measurement

        # Create tag strings
        tags = []
        for key, value in self.tags.items():
            escaped_name = self.__escape(key)
            escaped_value = self.__escape(value)

            tags.append("%s=%s" % (escaped_name, escaped_value))

        # Concatenate tags to current line protocol
        if len(tags) > 0:
            protocol = "%s,%s" % (protocol, ",".join(tags))

        # Create field strings
        values = []
        for key, value in self.values.items():
            escaped_name = self.__escape(key)
            escaped_value = self.__parse_value(value)
            values.append("%s=%s" % (escaped_name, escaped_value))

        # Concatenate fields to current line protocol
        protocol = "%s %s" % (protocol, ",".join(values))

        if self.timestamp is not None:
            protocol = "%s %d" % (protocol, self.timestamp)

        return protocol

    def __escape(self, value, escape_quotes=False):
        # Escape backslashes first since the other characters are escaped with
        # backslashes
        new_value = value.replace('\\', '\\\\')
        new_value = new_value.replace(' ', '\\ ')
        new_value = new_value.replace('=', '\\=')
        new_value = new_value.replace(',', '\\,')

        if escape_quotes:
            new_value = new_value.replace('"', '\\"')

        return new_value

    def __parse_value(self, value):
        if type(value) is int:
            return "%di" % value

        if type(value) is float:
            return "%g" % value

        if type(value) is bool:
            return value and "t" or "f"

        return "\"%s\"" % self.__escape(value, True)


if __name__ == '__main__':
    collection = MetricCollection()

    N = 200
    N2 = 100
    num_rows = 10000

    avg_time = 0
    avg_time_2 = 0

    for n in range(0, N):
        start = time.perf_counter()
        collection.clear()

        for i in range(0, num_rows):
            metric = Metric("weather")
            metric.with_timestamp(1465839830100400200)

            metric.add_value('CH_0', '0.0')
            metric.add_value('CH_1', '0.1')
            metric.add_value('CH_2', '0.2')
            metric.add_value('CH_3', '0.3')
            metric.add_value('CH_4', '0.4')
            metric.add_value('CH_5', '0.5')
            metric.add_value('CH_6', '0.6')
            metric.add_value('CH_7', '0.7')

            #data = {}
            #for i in range(0, 8):
            #    data["CH_" + str(i)] = 0.0 + 0.1 * i
            #metric.set_values(data)

            collection.append(metric)

        avg_time += time.perf_counter() - start

    avg_time /= N

    for n in range(0, N2):
        start = time.perf_counter()
        line_str = str(collection)
        avg_time_2 += time.perf_counter() - start

    avg_time_2 /= N2


    print(f"Creating      {num_rows} rows took {avg_time} seconds")
    print(f"Concatinating {num_rows} rows took {avg_time_2} seconds")
    #print(collection)
