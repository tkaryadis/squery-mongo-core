(ns squery-mongo-core.internal.convert.qoperators)

(defn remove-q-combine-fields [es]
  (let [es (mapv (fn [m] (if (and (map? m)
                                  (= (count m) 1)
                                  (contains? m "$__q__"))
                           (get m "$__q__")
                           m))
                 es)
        ;;unique-keys (distinct (apply concat (map keys es)))
        ]
    es
    #_(reduce (fn [uniq-es k]      ;;TODO problems, if many es, including $expr, hard to make to keep order also etc
              (let [vs (filter (fn [e] (contains? e k)) es)
                    vs-vals (map vals vs)
                    vs (apply merge (apply concat vs-vals))]
                (conj uniq-es {k vs})))
            []
            unique-keys)))