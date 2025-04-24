"""
Create execution stages by walking RDD lineage, splitting at shuffles.
"""

def create_stages(rdd):
    # 1) Gather transforms from root to this RDD
    lineage = []
    node = rdd
    while node and node.transform is not None:
        lineage.append((node.transform, node.ttype))
        node = node.prev
    lineage.reverse()

    # 2) Split into stages: accumulate until a 'reduceByKey'
    stages = []
    current = []
    for func, ttype in lineage:
        current.append((func, ttype))
        if ttype == 'reduceByKey':
            stages.append({'transforms': current.copy(), 'shuffle': True})
            current.clear()
    if current:
        stages.append({'transforms': current.copy(), 'shuffle': False})
    return stages