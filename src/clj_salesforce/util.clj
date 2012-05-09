(ns clj-salesforce.util
  (:import [java.net URLEncoder])
  (:require [clojure.string :as s]))

;; Set of models that come with Salesforce.  Require no thunking.
(def standard-object #{"Account" "Campaign" "Case" "Contact" "Contract" "Event" "Lead" "Opportunity"
                       "Product2" "Solution" "Task" "User"})

;; Set of fields default to Salesforce models.  Require no thunking.
(def system-fields #{"CreatedDate" "IsDeleted" "CreatedById" "Id" "Attributes" "Name"
                     "LastModifiedById" "LastModifiedDate" "SystemModstamp" "OwnerId"})

(defn salesforce-name
  "Generate API model names from keyword.  Override by passing your own exact string."
  ([model field] (if (keyword? field)
                   (let [nm (s/replace (name field) #"__s" "")
                         thunked (apply str (map s/capitalize (s/split nm #"-")))]
                     (str thunked (if (or (standard-object model) (system-fields thunked)) "" "__c")))
                   field))
  ([field] (salesforce-name nil field)))

(defn unsalesforce-names
  "Strip __c field declarations from symbol keys.  Also undoes any potential collisions with __s suffix."
  [ks]
  (let [trans-keys (map #(s/join "-" (map s/lower-case (butlast (re-seq #"[A-Z]*[^A-Z]*" (name %1))))) ks)
        short-keys (map #(s/replace %1 #"__c" "") trans-keys)
        overlapping (->> (reduce #(assoc %1 %2 (inc (%1 %2 0))) {} short-keys)
                         (filter (fn [[k c]] (> c 1)))
                         (map first)
                         set)]
    (map #(if (and (overlapping %1) (not (re-find #"__c" %2)))
            (keyword (str %1 "__s"))
            (keyword %1))
         short-keys trans-keys)))

(defn api-body-params
  "Prepare param hash for POST."
  ([params] (if (map? params)
              (apply dissoc (zipmap (map salesforce-name (keys params))
                                    (map api-body-params (vals params))) system-fields)
              params)))

(defn json-api-body
  "Prepare Salesforce params with body."
  [params] {:form-params (api-body-params params) :content-type :json})

(defn json-api-result
  "Makes Salesforce JSON calls more friendly."
  [result] (if (map? result)
             (zipmap (unsalesforce-names (keys result)) (map json-api-result (vals result)))
             result))

(defn object-path
  "Create REST URL for Salesforce objects"
  [type k v & fields]
  (let [base (if (= k :id)
               (format "/services/data/v24.0/sobjects/%s/%s" (salesforce-name type) v)
               (format "/services/data/v24.0/sobjects/%s/%s/%s"
                       (salesforce-name type) (salesforce-name k) (URLEncoder/encode v)))]
    (if (seq (filter identity fields))
      (str base "?fields=" (s/join "," (map salesforce-name fields)))
      base)))

(def salesforce-op identity)

(def salesforce-value identity)
