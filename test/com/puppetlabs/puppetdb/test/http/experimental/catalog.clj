(ns com.puppetlabs.puppetdb.test.http.experimental.catalog
  (:require [cheshire.core :as json]
            ring.middleware.params
            [com.puppetlabs.puppetdb.scf.storage :as scf-store]
            [com.puppetlabs.http :as pl-http]
            [com.puppetlabs.utils :as pl-utils])
  (:use clojure.test
        ring.mock.request
        [clj-time.core :only [now]]
        [com.puppetlabs.puppetdb.fixtures]
        [com.puppetlabs.puppetdb.examples]))

(use-fixtures :each with-test-db with-http-app)

(def c-t "application/json")

(defn get-request
  [path]
  (let [request (request :get path)]
    (update-in request [:headers] assoc "Accept" c-t)))

(defn get-diff
  ([] (get-diff nil))
  ([node-a node-b] (*app* (get-request (format "/experimental/catalog/%s/diff/%s" (name node-a) (name node-b))))))

(defn get-catalog
  ([]      (get-catalog nil))
  ([node] (*app* (get-request (str "/experimental/catalog/" node)))))

(defn is-response-equal
  "Test if the HTTP request is a success, and if the result is equal
to the result of the form supplied to this method."
  [response body]
  (is (= pl-http/status-ok   (:status response)))
  (is (= c-t (get-in response [:headers "Content-Type"])))
  (is (= (when-let [body (:body response)]
           (let [body (json/parse-string body)
                 resources (body "resources")]
             (-> body
               (update-in ["edges"] set)
               (assoc "resources" (into {} (for [[ref resource] resources]
                                             [ref (update-in resource ["tags"] sort)]))))))
         body)))

(deftest catalog-retrieval
  (let [basic-catalog (:basic catalogs)
        empty-catalog (:empty catalogs)]
    (scf-store/add-certname! (:certname basic-catalog))
    (scf-store/add-certname! (:certname empty-catalog))
    (scf-store/replace-catalog! basic-catalog (now))
    (scf-store/replace-catalog! empty-catalog (now))

    (testing "should return the catalog if it's present"
      (is-response-equal (get-catalog (:certname empty-catalog))
        {"name" (:certname empty-catalog)
         "resources" {"Class[Main]" {"certname"   (:certname empty-catalog)
                                     "type"       "Class"
                                     "title"      "Main"
                                     "resource"   "fc22ffa0a8128d5676e1c1d55e04c6f55529f04c"
                                     "exported"   false
                                     "sourcefile" nil
                                     "sourceline" nil
                                     "count"      1
                                     "tags"       ["class" "main"]
                                     "parameters" {"name" "main"}}
                     "Class[Settings]" {"certname"   (:certname empty-catalog)
                                        "type"       "Class"
                                        "title"      "Settings"
                                        "resource"   "cc1869f0f075fc3c3e5828de9e92d65a0bf8d9ff"
                                        "exported"   false
                                        "sourcefile" nil
                                        "sourceline" nil
                                        "count"      1
                                        "tags"       ["class" "settings"]
                                        "parameters" {}}
                     "Stage[main]" {"certname"   (:certname empty-catalog)
                                    "type"       "Stage"
                                    "title"      "main"
                                    "resource"   "124522a30c56cb9e4bbc66bae4c2515cda6ec889"
                                    "exported"   false
                                    "sourcefile" nil
                                    "sourceline" nil
                                    "count"      1
                                    "tags"       ["main" "stage"]
                                    "parameters" {}}}
         "edges" #{{"source" {"type" "Stage" "title" "main"}
                   "target" {"type" "Class" "title" "Settings"}
                   "relationship" "contains"}
                  {"source" {"type" "Stage" "title" "main"}
                   "target" {"type" "Class" "title" "Main"}
                   "relationship" "contains"}}}))

    (testing "should return status-not-found if the catalog isn't found"
      (let [response (get-catalog "non-existent-node")]
        (is (= pl-http/status-not-found (:status response)))
        (is (= {:error "Could not find catalog for non-existent-node"}
               (json/parse-string (:body response) true)))))

    (testing "should fail if no node is specified"
      (let [response (get-catalog)]
        (is (= pl-http/status-not-found (:status response)))
        (is (= "missing node") (:body response))))))

(deftest catalog-diffs
  (let [basic-catalog (:basic catalogs)
        empty-catalog (:empty catalogs)
        node-a (keyword (:certname basic-catalog))
        node-b (keyword (:certname empty-catalog))]
    (scf-store/add-certname! (name node-a))
    (scf-store/add-certname! (name node-b))
    (scf-store/replace-catalog! basic-catalog (now))
    (scf-store/replace-catalog! empty-catalog (now))

    (testing "should return the diff between the two catalogs"
      (let [{:keys [body status]} (get-diff node-a node-b)
            {:keys [resources edges] :as diff} (json/parse-string body true)]
        (is (= status pl-http/status-ok))
        (is (= (pl-utils/keyset diff) #{:resources :edges}))
        (is (= (pl-utils/keyset resources) #{:common node-a node-b}))
        (is (= (node-a resources) ["Class[foobar]" "File[/etc/foobar/baz]" "File[/etc/foobar]"]))
        (is (= (node-b resources) ["Class[Main]" "Class[Settings]" "Stage[main]"]))
        (is (empty? (:common resources)))
        (is (= (set (node-a edges))) #{{:source       {:type "Stage" :title "main"}
                                        :target       {:type "Class" :title "Settings"}
                                        :relationship "contains"}
                                       {:source       {:type "Stage" :title "main"}
                                        :target       {:type "Class" :title "Main"}
                                        :relationship "contains"}})

        (is (= (set (node-b edges)) #{{:source       {:type "Stage" :title "main"}
                                       :target       {:type "Class" :title "Settings"}
                                       :relationship "contains"}
                                      {:source       {:type "Stage" :title "main"}
                                       :target       {:type "Class" :title "Main"}
                                       :relationship "contains"}}))

        (is (empty? (:common edges)))))))
