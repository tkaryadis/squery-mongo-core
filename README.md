## Updating for MongoDB 7

- Java/Clojure is updated.  
- NodeJS/Clojurescript not yet(don't use).  
- Example apps not updated also(don't use).  

*project renamed from cmql to squery

## Leiningen dependencies

**Java or Clojure programmers sync driver** 

```
[org.squery/squery-mongo-core "0.2.0-SNAPSHOT"]
[org.squery/squery-mongoj "0.2.0-SNAPSHOT"]
```

**Java or Clojure programmers reactive driver** 

```
[org.squery/squery-mongo-core "0.2.0-SNAPSHOT"]
[org.squery/squery-mongoj-reactive "0.2.0-SNAPSHOT"]
```

**JS or Clojurescript programmers** use squery-mongojs

```
[org.squery/squery-mongo-core "0.2.0-SNAPSHOT"]
[org.squery/squery-mongojs "0.2.0-SNAPSHOT"]
```

## Getting Started

- [Documentation](https://squery.org/)
- [**Try it online, 400+ examples**](https://squery.org/playmongo)
- [SQuery discord server](https://discord.gg/zWDzp4B7Bf)

## SQuery

- MongoDB query language using up to **3x less code**  
- simple DSL inside a general programming language

## Usage

- as tool to **generate MQL** 
- **call SQuery** code directly from Java/NodeJS/Clojure/Clojurescript

## Example

```text
(q :people.workers
   (< :salary 1000)
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
client.db("people").collection("workets").aggregate(
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

## SQuery projects

- [org.squery/squery-mongo-core](https://github.com/tkaryadis/squery-mongo-core)
- [org.squery/squery-mongoj](https://github.com/tkaryadis/squery-mongoj)
- [org.squery/squery-mongoj-reactive](https://github.com/tkaryadis/squery-mongoj-reactive)
- [org.squery/squery-mongojs](https://github.com/tkaryadis/squery-mongojs)

## SQuery example apps

- [Clojure](https://github.com/tkaryadis/squery-mongo-app-clj)
- [Java](https://github.com/tkaryadis/squery-mongo-app-j)
- [Clojurescript](https://github.com/tkaryadis/squery-mongo-app-cljs)
- [NodeJS](https://github.com/tkaryadis/squery-mongo-app-js)


## License

Copyright © 2020-2023 Takis Karyadis.  
Distributed under the Eclipse Public License version 1.0.  
