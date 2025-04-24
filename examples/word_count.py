from mini_pyspark.context import MiniSparkContext

def map_word_to_pair(w): return (w,1)
def filter_alpha(pair): return pair[0].isalpha()
def sum_counts(a,b): return a+b
def split_to_letters(w): return list(w)
def format_kv(kv):
    # Convert (key, value) into a formatted string
    key, val = kv
    return [f"{key}->{val}"]

if __name__ == '__main__':
    sc = MiniSparkContext(num_workers=4)
    words = open('./sample.txt').read().split()
    # pipeline uses stage/DAG planning
    result = (
        sc.parallelize(words)
          .map(map_word_to_pair)
          .filter(filter_alpha)
          .reduceByKey(sum_counts)
          .flatMap(format_kv)
    )
    # Visualize the DAG before execution
    sc.visualize(result)
    result = result.collect()
    for line in result: print(line)
    sc.shutdown()