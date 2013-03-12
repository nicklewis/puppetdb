(ns com.puppetlabs.puppetdb.cats
  (:import [javax.imageio ImageIO])
  (:require [cheshire.core :as json])
  (:use [clojure.java.io :only [file]]))

(defn initialize-database!
  "Returns a map? corresponding to the catabase at `datadir`."
  [datadir]
  (System/setProperty "java.awt.headless" "true")
  (.mkdirs datadir)
  {:type :cats
   :source (filter #(re-matches #".*\.png$" %) (map str (file-seq (file datadir "sources"))))
   :datadir datadir})

(defn get-source-cat
  "Returns the contents of a random source cat file."
  [db]
  (-> (:source db)
      (rand-nth)
      (file)
      (ImageIO/read)
      (.getRaster)))

(defn cat-bytes
  [cat-raster]
  (let [width (.getWidth cat-raster)
        height (.getHeight cat-raster)
        byte-size (* width height 4)]
    (.getPixels cat-raster 0 0 width height (int-array byte-size))))

(defn replace-bytes
  [base-bytes content]
  (let [overlay-byte #(bit-or (bit-and (first %) 2r11111100) (last %))
        split-byte   (fn [byte] (map #(bit-and % 2r11) [(bit-shift-right byte 6) (bit-shift-right byte 4) (bit-shift-right byte 2) byte]))]
    (->> (concat (.getBytes content) [0])
         (mapcat split-byte)
         (map vector base-bytes)
         (map overlay-byte))))

(defn build-raster
  [cat-raster base-bytes replacement-bytes]
  (let [new-bytes (int-array (concat replacement-bytes (java.util.Arrays/copyOfRange base-bytes (count replacement-bytes) (count base-bytes))))
        width (.getWidth cat-raster)
        height (.getHeight cat-raster)]
    (.setPixels cat-raster 0 0 width height new-bytes)
    cat-raster))

(defn assemble-byte
  [[a b c d]]
  (byte (apply bit-or (map #(bit-shift-left (first %) (last %)) (map vector (map #(bit-and % 2r11) [a b c d]) [6 4 2 0])))))

(defn retrieve-catalog
  [{:keys [datadir]} certname]
  (let [location (file datadir (format "%s.png" certname))
        raster (.getRaster (ImageIO/read location))
        catalog-bytes (cat-bytes raster)]
    (String. (byte-array (take-while #(not (= % 0)) (map assemble-byte (partition 4 catalog-bytes)))))))

(defn save-catalog!
  [{:keys [datadir] :as db} certname catalog]
  {:pre [(map? db)
         datadir]}
  (let [content (json/generate-string catalog)
        cat-raster (get-source-cat db)
        base-bytes (cat-bytes cat-raster)
        dest-file (file datadir (format "%s.png" certname))
        replacement-bytes (replace-bytes base-bytes content)
        output-raster (build-raster cat-raster base-bytes replacement-bytes)
        output-png (doto (java.awt.image.BufferedImage. (.getWidth output-raster) (.getHeight output-raster) java.awt.image.BufferedImage/TYPE_INT_ARGB)
                     (.setData output-raster))]
    (ImageIO/write output-png "png" dest-file)))
