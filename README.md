## cMQL

- query and data processing language for MongoDB
- generates MQL that can be used as standalone commands, or as arguments in driver methods
- main characteristics
  - up to **3x** less code
  - simple structure of code
  - simple notation
- () for code , {} for data       
  it is like MQL with ()
- portable queries in both cmql-js and cmql-java      
- with the same performance

It can be used as a tool to generate MQL or to call cMQL code directly.

## Getting Started

- [**Documentation**](http://cmql.org/)
- [**Try it online, see examples**](http://cmql.org/play)

## Leiningen dependencies

**Java or Clojure programmers**

```
[cmql/cmql-core "0.1.0-SNAPSHOT"]
[cmql/cmql-j "0.1.0-SNAPSHOT"]
```

**JS or Clojurescript programmers**

```
[cmql/cmql-core "0.1.0-SNAPSHOT"]
[cmql/cmql-js "0.1.0-SNAPSHOT"]
```

## Example

```clojure
(q (< :salary 1000)
   (> :years 1)
   {:children (filter (fn [:child.] (< :child.age. 15)) :all-children)}
   (>= :children 2)
   {:bonus (reduce (fn [:total. :child.]
                     (cond (< :child.age. 5) (+ :total. 100)
                           (< :child.age. 10) (+ :total. 50)
                           :else (+ :total. 20)))
                   0
                   :children)}
   [:!id :name {:new-salary (+ :salary (if- (> :bonus 200) 200 :bonus))}]
   (sort :!new-salary))
```

Generates

```js
aggregate(
[{"$match":
   {"$expr":
     {"$and": [{"$lt": ["$salary", 1000]}, {"$gt": ["$years", 1]}]}}},
  {"$set":
    {"children":
      {"$filter":
        {"input": "$all-children",
          "cond": {"$lt": ["$$child.age", 15]},
          "as": "child"}}}},
  {"$match": {"$expr": {"$gte": ["$children", 2]}}},
  {"$set":
    {"bonus":
      {"$reduce":
        {"input": "$children",
          "initialValue": 0,
          "in":
          {"$let":
            {"vars": {"total": "$$value", "child": "$$this"},
              "in":
              {"$switch":
                {"branches":
                  [{"case": {"$lt": ["$$child.age", 5]},
                    "then": {"$add": ["$$total", 100]}},
                   {"case": {"$lt": ["$$child.age", 10]},
                    "then": {"$add": ["$$total", 50]}}],
                  "default": {"$add": ["$$total", 20]}}}}}}}}},
  {"$project":
    {"id": 0,
      "name": 1,
      "new-salary":
      {"$add":
        ["$salary", {"$cond": [{"$gt": ["$bonus", 200]}, 200, "$bonus"]}]}}},
  {"$sort": {"new-salary": -1}}])
```


