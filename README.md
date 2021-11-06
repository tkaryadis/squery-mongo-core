## cMQL

- query and data processing language for MongoDB
- up to **3x** less code
- simple structure of code
- simple notation

It can be used as a tool to generate MQL or to call cMQL code directly.

## Getting Started

- [**Documentation**](http://cmql.org/)
- [**Try it online, see more then 100 examples**](http://cmql.org/play)

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
(q (= :bedrooms 1)
   (= :country.code "GR")
   (group {:_id :stars}
          {:average-price (avg :price)})
   (sort :average-price)
   (limit 1))
```

Generates

```js
aggregate(
[{"$match":
   {"$expr":
     {"$and":
       [{"$eq": ["$bedrooms", 1]},
        {"$eq": ["$country.code", "GR"]}]}}},
 {"$group": {"_id": "$stars",
             "average-price": {"$avg": "$price"}}},
 {"$sort": {"average-price": 1}},
 {"$limit": 1}])
```


