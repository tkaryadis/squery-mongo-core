## cMQL  

- query and data processing language for MongoDB  
- generates MQL that can be used as standalone commands, or as arguments in driver methods
- main characteristics
  - up to **3x** less code
  - simple structure of code
  - simple notation
- () for code , {} for data       
  like MQL with () [see also](/docs/intro/why)      
- portable queries in both cmql-js and cmql-java      
- with the same perfomance          

It can be used as a tool to generate MQL or to call cMQL code directly.    

### Java and JS drivers support  

For java
- `[cmql/cmql-core "0.1.0-SNAPSHOT"]`  
- `[cmql/cmql-j "0.1.0-SNAPSHOT"]`  

For nodejs
- `[cmql/cmql-core "0.1.0-SNAPSHOT"]`  
- `[cmql/cmql-js "0.1.0-SNAPSHOT"]`  

### Example

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


