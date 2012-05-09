(ns clj-salesforce.core
  (:use clojure.set)
  (:use slingshot.slingshot)
  (:require [clojure.string :as s])
  (:require [clj-http.client :as http])
  (:use clj-salesforce.util))

(defn api-login
  "Get session ID and server from Salesforce for API calls.  Returns login token passed to other api calls."
  ([client-id client-secret username password security-token]
     (let [params {:grant_type "password"
                   :client_id client-id
                   :client_secret client-secret
                   :username username
                   :password (str password security-token)
                   :format "json"}
           resp (http/post "https://login.salesforce.com/services/oauth2/token" {:form-params params :as :json})]
       (:body resp)))
  ([{:keys [client-id client-secret username password security-token]}]
     (api-login client-id client-secret username password security-token)))

(defn api-request
  "Builder for Salesforce REST requests."
  ([token method path opts]
     (if token
       (http/request (merge {:method method :url (str (:instance_url token) path) :as :json}
                            (merge-with #(if (map? %1) (merge %1 %2) %2)
                                        {:headers {"Authorization" (str "OAuth " (:access_token token))}}
                                        (if (= :patch method) {:query-params {"_HttpMethod" "PATCH"} :method :post})
                                        opts)))
       (throw+ {:nil-token true :message "No token supplied to connect to API."})))
  ([token method path] (api-request token method path {})))

(def json-api-request (comp json-api-result :body api-request))

(defn salesforce-resources
  "Overview of Salesforce Resources"
  [token] (:body (api-request token :get "/services/data/v24.0/")))

(defn custom-objects-list
  "Retrieve custom object from Salesforce DB."
  ([token] (json-api-request token :get (str "/services/data/v24.0/sobjects/")))
  ([token & objs] (filter #((set (map salesforce-name objs)) (salesforce-name (:name %1)))
                          (:sobjects (custom-objects-list token)))))

(defn describe-object
  "Pull object description from salesforce."
  ([token obj fmt]
     (api-request token :get (format "/services/data/v24.0/sobjects/%s/describe" (salesforce-name obj)) {:as fmt}))
  ([token obj] (describe-object token obj :json)))

(defn object-schema
  "Get base object schema for Salesforce object"
  ([token obj & {:keys [raw include-sys property]}]
     (let [description (:body (describe-object token (salesforce-name obj) :json-string-keys))
           field-list (description "fields")
           raw-schema (into {} (map #(vector (%1 "name")
                                             (cond
                                              (set? property) (zipmap property (map %1 property))
                                              (coll?) (map %1 property)
                                              :else (%1 (or property "type"))))
                                    field-list))
           unsys (if include-sys raw-schema (apply dissoc raw-schema system-fields))]
       (if raw unsys (zipmap (unsalesforce-names (keys unsys)) (vals unsys)))))
  ([token obj] (object-schema token obj :raw false :include-sys true)))

(defn get-object
  "Select objects by id"
  [token obj id & fields]
  (json-api-request token :get (object-path obj :id id fields)))

(defn create-object
  "Post object to server."
  [token obj body]
  (api-request token :post (str "/services/data/v24.0/sobjects/" (salesforce-name obj)) (json-api-body body)))

(defn update-object
  "Update give object id by parameters."
  [token obj id body]
  (api-request token :patch (object-path obj :id id) (json-api-body body)))

(defn upsert-object
  "Upsert (insert or update) an object given an external id."
  [token obj k v body]
  (if (= k :id)
    (throw (IllegalArgumentException. "Cannot upsert object by internal salesforce ID."))
    (api-request token :patch (object-path obj k v) (json-api-body body))))

(defn save-object
  "Create or update object depending on whether id is present in body"
  [token obj body]
  (if (:id obj)
    (update-object token obj (:id obj) (dissoc body :id))
    (create-object token obj body)))

(defn select-objects
  "Execute a SOQL expression.  Optional builder. Automatically includes ID.  Lazy sequence over all nexts."
  ([token obj conditions & fields]
     (if (seq fields)
       (let [table (salesforce-name obj)
             where (if (empty? conditions)
                     ""
                     (str "where " (s/join " and " (for [[k v] conditions]
                                                     (cond
                                                      (map? v) (format "%s %s %s"
                                                                       (salesforce-name k)
                                                                       (:op v)
                                                                       (salesforce-value (:val v)))
                                                      (string? v) (format "%s = '%s'" (salesforce-name k) v)
                                                      :else (format "%s = %s" (salesforce-name k) v))))))
             field-list (s/join ", " (cons "id" (filter #(not= (s/lower-case (name %1)) "id")
                                                        (map salesforce-name fields))))]
         (select-objects token (format "select %s from %s %s" field-list table where)))
       (apply select-objects token obj conditions (keys (object-schema token obj)))))
  ([token query]
     (let [result (api-request token :get (format "/services/data/v24.0/query/?q=%s" (URLEncoder/encode query)))
           extractor identity]
       (mapcat (fn [results] (map json-api-result (get-in results [:body :records])))
               (take-while identity (iterate #(when-let [next-url (:nextRecordUrl %1)]
                                                (api-request token :get next-url))
                                             result))))))

(defn delete-object
  "Delete object by id."
  [token obj id] (api-request token :delete (object-path obj :id id)))
