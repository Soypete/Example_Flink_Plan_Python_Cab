from org.apache.flink.api.common.functions import FlatMapFunction, ReduceFunction, FilterFunction
from org.apache.flink.api.java.functions import KeySelector
from org.apache.flink.streaming.api.windowing.time.Time import milliseconds

# find destination count!
# select destination column
class SelectDestination(FilterFunction):
    def filter(self, input):
        return input[7]

# created the tuple round the word for counting
class Tupler(FlatMapFunction):
    def flatMap(self, value, collector):
        collector.collect((1, value))

class Selector(KeySelector):
    def getKey(self, input):
        return input[1]

# count occurances by increasing tuple number
class CountDest(ReduceFunction):
    def map(self, input1, input2):
        count1, word1 = input1
        count2, word2 = input2
        return (count1 + count2, word1)
    
# run by flink python execution layer
def main(flink):
    # create flink environment. This environment gives you access to the flink pre-defined operators. 
    env = flink.get_execution_environment()
    # read in cab text file. This creates a data stream.
    # Data format
    # cab_id|cab_type|cab_number_plate|cab_driver_name|ongoing_trip/not|pickup_location|destination|passenger_count
    text = env.read_text_file("file:///Users/miriah.peterson@getweave.com/code/flink_tutorial/assignment_1/cab-flink.txt") # likes full source path

    text.filter(SelectDestination()) \
        .flat_map(Tupler()) \
        .write_as_text("file:///Users/miriah.peterson@getweave.com/code/flink_tutorial/assignment_1/cab-out.txt")
        # .key_by(Selector()) \
        # .time_window(milliseconds(50)) \
        # .reduce(CountDest()) \
        # .output()

    result = env.execute("cab example")
    print(result)
