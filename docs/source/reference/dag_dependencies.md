(dag-dependencies)=

# DAG Dependencies

Often, parallel workflow is described in terms of a [Directed Acyclic Graph](https://en.wikipedia.org/wiki/Directed_acyclic_graph)
or DAG. A popular library for working with Graphs is [NetworkX]. Here, we will walk through
a demo mapping a NetworkX DAG to task dependencies.

The full script that runs this demo can be found in {file}`examples/parallel/dagdeps.py`.

## Why are DAGs good for task dependencies?

The 'G' in DAG is 'Graph'. A Graph is a collection of **nodes** and **edges** that connect
the nodes. For our purposes, each node would be a task, and each edge would be a
dependency. The 'D' in DAG stands for 'Directed'. This means that each edge has a
direction associated with it. So we can interpret the edge (a,b) as meaning that b depends
on a, whereas the edge (b,a) would mean a depends on b. The 'A' is 'Acyclic', meaning that
there must not be any closed loops in the graph. This is important for dependencies,
because if a loop were closed, then a task could ultimately depend on itself, and never be
able to run. If your workflow can be described as a DAG, then it is impossible for your
dependencies to cause a deadlock.

## A sample DAG

Suppose we have five tasks, and they depend on each other as follows:

- Task 0 has no dependencies and can run right away.
- When 0 finishes, tasks 1 and 2 can start.
- When 1 finishes, task 3 must wait for 2, but task 4 can start right away.
- When 2 finishes, task 3 can finally start.

We might represent these tasks with a simple graph:

```{figure} figs/simpledag.*
:width: 600px
```

An arrow is repsented here by a fattened bit on the edge. As we specified when we made the
edges, we can see that task 0 depends on nothing, and can run immediately. 1 and 2 depend
on 0; 3 depends on 1 and 2; and 4 depends only on 1.

Let's construct this 5-node DAG. First, we create a directed graph or 'digraph':

```ipython
In [1]: import networkx as nx

In [2]: G = nx.DiGraph()

In [3]: map(G.add_node, range(5))  # Add 5 nodes, labeled 0-4.
```

Now we can add edges:

```ipython
In [4]: G.add_edge(0, 1)  # Task 1 depends on task 0...

In [5]: G.add_edge(0, 2)  # ...and so does task 2.

In [6]: G.add_edge(1, 3)  # Task 3 depends on task 1...

In [7]: G.add_edge(2, 3)  # ...and also on task 2.

In [8]: G.add_edge(1, 4)  # Task 4 depends on task 1.
```

The following code produces the figure above:

```ipython
In [9]: pos = {0: (0, 0), 1: (1, 1), 2: (-1, 1),
   ...:        3: (0, 2), 4: (2, 2)}
In [10]: nx.draw(G, pos, edge_color='r')
```

Taking failures into account, assuming all dependencies are run with the default
`success=True, failure=False`, the following cases would occur for each node's failure:

0. All other tasks fail as impossible.
1. 2 can still succeed, but 3 and 4 are unreachable.
2. 3 becomes unreachable, but 4 is unaffected.
3. and 4. are terminal, and can have no effect on other nodes.

Let's look at a larger, more complex network.

## Computing with a random DAG

For demonstration purposes, we have a function that generates a random DAG with a given
number of nodes and edges.

```{literalinclude} ../examples/dagdeps.py
:language: python
:lines: 24-40
```

So first, we start with a graph of 32 nodes, with 128 edges:

```ipython
In [11]: G = random_dag(32, 128)
```

Then we need some jobs. In reality these would all be different, but for our toy example
we'll use a single function that sleeps for a random interval:

```{literalinclude} ../examples/dagdeps.py
:language: python
:lines: 16-21
```

Now we can build our dict of jobs corresponding to the nodes on the graph:

```ipython
In [12]: jobs = {n: randomwait for n in G}
```

Once we have a dict of jobs matching the nodes on the graph, we can start submitting jobs,
and linking up the dependencies. Since we don't know a job's msg_id until it is submitted,
which is necessary for building dependencies, it is critical that we don't submit any jobs
before other jobs it may depend on. Fortunately, NetworkX provides a
{meth}`topological_sort` method which ensures exactly this. It presents an iterable, that
guarantees that when you arrive at a node, you have already visited all the nodes on which
it depends:

```ipython
In [13]: import ipyprallel as ipp

In [14]: rc = ipp.Client()

In [15]: view = rc.load_balanced_view()

In [16]: results = {}

In [17]: for node in nx.topological_sort(G):
    ...:     # Get list of AsyncResult objects from nodes
    ...:     # leading into this one as dependencies.
    ...:     deps = [results[n] for n in G.predecessors(node)]
    ...:     # Submit and store AsyncResult object.
    ...:     with view.temp_flags(after=deps, block=False):
    ...:         results[node] = view.apply(jobs[node])
```

Now that we have submitted all the jobs, we can wait for the results:

```ipython
In [18]: view.wait(results.values())
```

Now, at least we know that all the jobs ran and did not fail. But we don't know that
the ordering was properly respected. For this, we can use the {attr}`metadata` attribute
of each AsyncResult.

## Inspecting the metadata

These objects store a variety of metadata about each task, including various timestamps.
We can validate that the dependencies were respected by checking that each task was
started after all of its predecessors were completed:

```{literalinclude} ../examples/dagdeps.py
:language: python
:lines: 72-81
```

Then we can call this function:

```ipython
In [19]: validate_tree(G, results)
```

It produces no output but the assertions pass.

We can also validate the graph visually. By drawing the graph with each node's x-position
as its start time, all arrows must be pointing to the right if dependencies were respected.
For spreading, the y-position will be the runtime of the task, so long tasks
will be at the top, and quick, small tasks will be at the bottom.

```ipython
In [20]: from matplotlib.dates import date2num

In [21]: from matplotlib.cm import gist_rainbow

In [22]: pos, colors = {}, {}

In [23]: for node in G:
    ...:     md = results[node].metadata
    ...:     start = date2num(md.started)
    ...:     runtime = date2num(md.completed) - start
    ...:     pos[node] = (start, runtime)
    ...:     colors[node] = md.engine_id

In [24]: nx.draw(G, pos, nodelist=colors.keys(), node_color=list(colors.values()),
    ...:     cmap=gist_rainbow)
```

```{figure} figs/dagdeps.*
:width: 600px

Time started on the x-axis, runtime on the y-axis, and color-coded by engine-id (in this
case there were four engines). Edges denote dependencies. The limits of the axes have been
adjusted in this plot.
```

[networkx]: https://networkx.org
