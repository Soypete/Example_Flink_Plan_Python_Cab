from org.apache.flink.streaming.api.functions.source import SourceFunction
from org.apache.flink.api.common.functions import FlatMapFunction, ReduceFunction
from org.apache.flink.api.java.functions import KeySelector
from org.apache.flink.streaming.api.windowing.time.Time import milliseconds

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
    
# # calculate the average number of passengers

# class GroupBy(GroupbyFunction):
#     def group_by(self, input):
#         return (input[6])

# class AvePass(ReduceFunction):
#     def reduce(self, input1, input2):
#         count1, word1 = input1
#         count2, word2 = input2
#         return (count1 + count2, word1)

# run by flink python execution layer
def main(factory):
    # create flink environment. This environment gives you access to the flink pre-defined operators. 
    env = factory.get_execution_environment()
    # read in cab text file. This creates a data stream.
    # Data format
    # cab_id|cab_type|cab_number_plate|cab_driver_name|ongoing_trip/not|pickup_location|destination|passenger_count
    text = env.read_text_file("file:///cab-flink.txt") \
        .filer(FilterDestination()) \
        .map(CountDest()) \
        .time_window(milliseconds(50)) \
        .pritn()\
    # text.write_text_file("file:///parsed_destinations.txt")

    # text1 = env.read_text_file("file:///cab-flink.txt")         
    #     .group_by(GroupBy()) \
    #     .reduce(AvePass())
    #     .time_window(milliseconds(50)) \
    # text1.write_text_file("file:///parsed_passengers.txt")

    # text2 = env.read_text_file("file:///cab-flink.txt")         
    #     .reduce(Sum()) \
    #     .output()
    #     .time_window(milliseconds(50)) \
    # text2.write_text_file("file:///parsed_trips.txt")
    env.execute()
