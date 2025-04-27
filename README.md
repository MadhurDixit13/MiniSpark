# MiniSpark

MiniSpark is a minimal, educational re-implementation of Apache Spark in pure Python. It uses the `multiprocessing` module to simulate a driver and worker cluster, supports lazy transformations and actions, stage/DAG planning, and simple DAG visualization.

## 🚀 Features

- **Core RDD API**  
  - `map`, `filter`, `flatMap`, `sample`  
  - `reduceByKey`, `join`, `count`, `collect`, `take`, etc.  
- **Lazy Evaluation & Stage Planning**  
  - Builds a lineage DAG of transformations  
  - Splits at shuffle boundaries into narrow vs. wide stages  
- **Multiprocessing Scheduler**  
  - Driver dispatches tasks to worker processes  
  - Timeout-based retry on worker failure with respawn (fault-tolerance)  
  - Graceful shutdown  
- **Persistence & Caching**  
  - `rdd.cache()` to memoize results and skip re-computation on repeated actions  
- **DAG Visualization** (optional)  
  - Uses `networkx` + Graphviz (or spring-layout fallback)  
  - Annotates narrow vs. wide transformations  
- **Examples**  
  - Word count, sampling, letter count  
  - DAG visualization demo  

## 📦 Installation

1. **Clone the repo**  
   ~~~bash
   git clone https://github.com/MadhurDixit13/MiniSpark.git
   cd mini_pyspark
   ~~~  

2. **Install in editable mode**  
   ~~~bash
   pip install -e .
   ~~~  

3. **(Optional) Install extras** for visualization  
   ~~~bash
   pip install networkx matplotlib pygraphviz
   ~~~  

## 🎓 Quick Start

1. **Run the word-count example**  
   ~~~bash
   python -m examples.word_count.py
   ~~~  

## 📂 Project Structure

```
minispark/
├── mini_pyspark/
│   ├── context.py      # SparkContext, visualize(), plan()
│   ├── rdd.py          # RDD class with lazy transforms + cache
│   ├── scheduler.py    # TaskScheduler with stage execution and with fault-tolerance
│   ├── worker.py       # Worker loop for map/filter tasks with failure simulation
│   ├── planner.py      # Lineage walker & stage splitter
│   └── viz.py          # NetworkX/Graphviz DAG drawing
├── examples/
│   ├── word_count.py        # Core word-count demo
├── sample.txt         # Sample data for examples
└── README.md
```

## 📖 How It Works

1. **Driver** builds an `RDD` lineage with lazy transforms.  
2. **Scheduler** requests stages from `planner.py`, dispatches narrow transforms to workers, then performs shuffle & reduce centrally.  
3. **Workers** apply `map`/`filter`/`flatMap`/`sample` functions in parallel.  
4. **Collect** gathers results, groups by key, and applies `reduceByKey`.  
5. **Visualization** can render the DAG before computation.

## 🤝 Contributing

1. Fork the repo  
2. Create a feature branch  
3. Implement & test your feature  
4. Submit a Pull Request

Please adhere to the existing code style and add examples/tests for new functionality.

## 📄 License

This project is released under the GNU General Purpose License. See [LICENSE](LICENSE) for details.  

 
