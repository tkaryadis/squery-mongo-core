## cMQL

- query and data processing language for MongoDB
- up to **3x** less code
- simple structure
- simple notation

Usage

- as tool to **generate MQL** usable from all drivers
- to **call cMQL** code directly from Java/NodeJS/Clojure/Clojurescript

## Getting Started

- [Documentation](https://cmql.org/)
- [**Try it online, see many examples**](https://cmql.org/play)
- [cMQL chat server](https://discord.gg/zWDzp4B7Bf)

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

## cMQL projects

- [org.cmql/cmql-core](https://github.com/tkaryadis/cmql-core)
- [org.cmql/cmql-j](https://github.com/tkaryadis/cmql-j)
- [org.cmql/cmql-js](https://github.com/tkaryadis/cmql-js)

**cMQL example apps**

- [Clojure](https://github.com/tkaryadis/cmql-app-clj)
- [Java](https://github.com/tkaryadis/cmql-app-j)
- [Clojurescript](https://github.com/tkaryadis/cmql-app-cljs)
- [NodeJS](https://github.com/tkaryadis/cmql-app-js)

## Leiningen dependencies

**Java or Clojure programmers** use cmql-j

```
[org.cmql/cmql-core "0.1.0-SNAPSHOT"]
[org.cmql/cmql-j "0.1.0-SNAPSHOT"]
```

**JS or Clojurescript programmers** use cmql-js

```
[org.cmql/cmql-core "0.1.0-SNAPSHOT"]
[org.cmql/cmql-js "0.1.0-SNAPSHOT"]
```

## License

Copyright © 2020,2021 Takis Karyadis.  
Distributed under the Eclipse Public License either version 1.0.

