## SQuery

- query and data processing language for MongoDB
- up to **3x** less code
- simple structure
- simple notation

Usage

- as tool to **generate MQL** usable from all drivers
- to **call SQuery** code directly from Java/NodeJS/Clojure/Clojurescript

## Getting Started

- [Documentation](https://squery.org/)
- [**Try it online, see many examples**](https://squery.org/playmongo)
- [SQuery chat server](https://discord.gg/zWDzp4B7Bf)

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

## SQuery projects

- [org.squery/squery-core](https://github.com/tkaryadis/squery-mongo-core)
- [org.squery/squery-j](https://github.com/tkaryadis/squery-mongo-j)
- [org.squery/squery-js](https://github.com/tkaryadis/squery-mongo-js)

**SQuery example apps**

- [Clojure](https://github.com/tkaryadis/squery-mongo-app-clj)
- [Java](https://github.com/tkaryadis/squery-mongo-app-j)
- [Clojurescript](https://github.com/tkaryadis/squery-mongo-app-cljs)
- [NodeJS](https://github.com/tkaryadis/squery-mongo-app-js)

## Leiningen dependencies

**Java or Clojure programmers** use squery-mongo-j

```
[org.squery/squery-mongo-core "0.2.0-SNAPSHOT"]
[org.squery/squery-mongo-j "0.2.0-SNAPSHOT"]
```

**JS or Clojurescript programmers** use squery-mongo-js

```
[org.squery/squery-mongo-core "0.2.0-SNAPSHOT"]
[org.squery/squery-mongo-js "0.2.0-SNAPSHOT"]
```

## License

Copyright © 2020,2022 Takis Karyadis.  
Distributed under the Eclipse Public License version 1.0.
