# An Efficient Data Pipeline for Fraud Detection

## Methodology
To design an efficient fraud detection feature for PayMo users; the following are the two steps of solution:

1. Building model: Using the batch_payment data to construct efficient graph data structure
1. Streaming Values: For each incoming payments searching/parsing through the model to find out the degree.

## Evaluated data structures
1. Representing the transactions graph: To represent the transaction the following data structures have been evaluated:

	1. Bloom Filters: False positive is bad
	1. Trees(AVL,RB): Maintaining N trees is costly with no real advantage.
	1. Hashmaps: O(1) access for membership queries
	1. Array: duh!
From the above mentioned data structures; Hashmaps provide us with a good performance while performing membership queries and to represent a graph, a hash map array is used for each node in graph.

## Implementation


## Analysis
1. BFS: O(|V|+|E|)


## Testing
![../images/test.gif](../images/test.gif)

## Results

## Conclusion
