from mini_pyspark.context import MiniSparkContext

def map_word_to_pair(w):
    return (w, 1)

def filter_alpha(pair):
    return pair[0].isalpha()

def sum_counts(a, b):
    return a + b

if __name__ == "__main__":
    sc = MiniSparkContext(num_workers=4)

    with open('sample.txt') as f:
        words = f.read().split()

    counts = (
        sc.parallelize(words)
          .map(map_word_to_pair)
          .filter(filter_alpha)
          .reduceByKey(sum_counts)   # <-- new aggregation step
          .collect()
    )

    # Sort and print nicely
    for word, cnt in sorted(counts):
        print(f"{word}: {cnt}")

    sc.shutdown()
