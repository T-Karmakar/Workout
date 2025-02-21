package com.tk.explore

class GeneticSearch {

}

object GeneticSearch extends App {

  import scala.util.Random

  // Graph Representation (Adjacency List with Weights)
  private val graph: Map[String, List[(String, Int)]] = Map(
    "A" -> List(("B", 5), ("C", 2)),
    "B" -> List(("D", 2), ("E", 3)),
    "C" -> List(("E", 7), ("F", 3)),
    "D" -> List(("G", 1)),
    "E" -> List(("G", 3)),
    "F" -> List(("G", 4))
  )

  // Generate a random path from A to G
  private def generateRandomPath(): List[String] = {
    var path = List("A")
    var current = "A"

    while (current != "G") {
      val neighbors = graph.getOrElse(current, List())
      if (neighbors.isEmpty) return path // No path found, return as is
      val (nextNode, _) = neighbors(Random.nextInt(neighbors.length)) // Pick a random neighbor
      path = path :+ nextNode
      current = nextNode
    }
    path
  }

  // Compute fitness (Lower distance = Higher fitness)
  private def computeFitness(path: List[String]): Double = {
    val distance = path.sliding(2).map {
      case List(a, b) => graph(a).find(_._1 == b).map(_._2).getOrElse(Int.MaxValue)
      case _ => 0
    }.sum

    if (distance == Int.MaxValue) 0 else 1.0 / distance // Fitness = 1/Distance
  }

  // Selection (Roulette Wheel)
  private def selectParents(population: List[List[String]]): (List[String], List[String]) = {
    val totalFitness = population.map(computeFitness).sum
    val probabilities = population.map(p => computeFitness(p) / totalFitness)

    def rouletteSelect(): List[String] = {
      val rand = Random.nextDouble()
      var cumulative = 0.0
      for ((p, prob) <- population.zip(probabilities)) {
        cumulative += prob
        if (rand < cumulative) return p
      }
      population.head
    }

    (rouletteSelect(), rouletteSelect())
  }

  // Crossover (Swap Paths)
  private def crossover(parent1: List[String], parent2: List[String]): List[String] = {
    val crossoverPoint = Random.nextInt(math.min(parent1.length, parent2.length))
    val newPath = parent1.take(crossoverPoint) ++ parent2.drop(crossoverPoint)
    if (newPath.last != "G") newPath :+ "G" else newPath
  }

  // Mutation (Randomly Change a Node)
  private def mutate(path: List[String]): List[String] = {
    if (Random.nextDouble() < 0.2) { // 20% chance of mutation
      val mutationPoint = Random.nextInt(path.length - 1) + 1
      val currentNode = path(mutationPoint - 1)
      val newNeighbors = graph.getOrElse(currentNode, List()).map(_._1)
      if (newNeighbors.nonEmpty) {
        val newPath = path.updated(mutationPoint, newNeighbors(Random.nextInt(newNeighbors.length)))
        if (newPath.last != "G") newPath :+ "G" else newPath
      } else path
    } else path
  }

  // Main Genetic Algorithm Function
  private def geneticAlgorithm(iterations: Int, populationSize: Int): List[String] = {
    var population = (1 to populationSize).map(_ => generateRandomPath()).toList

    for (_ <- 1 to iterations) {
      val newPopulation = (1 to populationSize / 2).flatMap { _ =>
        val (parent1, parent2) = selectParents(population)
        val child1 = mutate(crossover(parent1, parent2))
        val child2 = mutate(crossover(parent2, parent1))
        List(child1, child2)
      }
      population = newPopulation.toList
    }

    // Return best path
    population.minBy(computeFitness)
  }

  // Run the Genetic Algorithm
  private val bestPath = geneticAlgorithm(100, 20) // 100 iterations, 20 population size

  println(s"Best Path Found: ${bestPath.mkString(" -> ")}")
  println(s"Best Path Distance: ${1.0 / computeFitness(bestPath)}")

}
