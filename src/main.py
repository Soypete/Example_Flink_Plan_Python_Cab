from org.apache.flink.streaming.api.functions.source import SourceFunction
from org.apache.flink.api.common.functions import FlatMapFunction, ReduceFunction
from org.apache.flink.api.java.functions import KeySelector
from org.apache.flink.streaming.api.windowing.time.Time import milliseconds




class Reduce(ReduceFunction):
    def reduce(self):

# find destination count!
        
# get destination 
class FilterDestination(FilterFunction):
    def filterDestination(self, input):
        return input[7]

# counts the number of times that a destination occurs and returns the destination word and count in a "tuple". 
class CountDest(MapFunction):
    def map(self, input1, input2):
        count1, word1 = input1
        count2, word2 = input2
        return (count1 + count2, word1)
    
# calculate the average number of passengers

class FilterPassengers(FilterFunction):
    def filterDestination(self, input):
        return input[7]


class GroupBy(GroupbyFunction):
    def groupBy:

class MaxBy(MaxbyFunction):
    def maxby:


class Sum(ReduceFunction):
    def reduce(self, input1, input2):
        count1, word1 = input1
        count2, word2 = input2
        return (count1 + count2, word1)

# run by flink python execution layer
def main(factory):
    # create flink environment. This environment gives you access to the flink pre-defined operators. 
    env = factory.get_execution_environment()
    # read in cab text file. This creates a data stream.
    # Dataformat
    # cab_id|cab_type|cab_number_plate|cab_driver_name|ongoing_trip/not|pickup_location|destination|passenger_count
    text = env.read_text_file("file:///cab-flink.txt") \
        .flat_map(Tokenizer()) \
        .key_by(Selector()) \
        .time_window(milliseconds(50)) \
        .reduce(Sum()) \
        .output()
    env.execute()
    text.write_text_file("file:///parsed.txt")
