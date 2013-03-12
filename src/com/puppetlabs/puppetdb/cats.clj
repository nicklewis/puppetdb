(ns com.puppetlabs.puppetdb.cats
  (:import [javax.imageio ImageIO])
  (:require [cheshire.core :as json])
  (:use [clojure.java.io :only [file]]))

(gen-class
  :name com.puppetlabs.puppetdb.cats.StegoStream
  :extends java.io.InputStream
  :prefix "stego-"
  :state state
  :init init
  :constructors {[java.io.File] []}
  :exposes-methods {read readSuper})

(defn stego-init
  [input-file]
  (let [raster (.getRaster (ImageIO/read input-file))]
    [[] (ref {:raster raster
              :pixel 0
              ;; The number of "data" bits contained per byte.
              :bits-per-byte 2
              ;; The number of "data" bytes contained per pixel.
              :bytes-per-pixel 1
              :done? false
              :width (.getWidth raster)
              :height (.getHeight raster)})]))

(defn- index->coords
  [index width]
  [(mod index width) (int (/ index width))])

(defn- assemble-byte
  [bytes]
  (byte (apply bit-or (map #(bit-shift-left (first %) (last %)) (map vector (map #(bit-and % 2r11) bytes) [6 4 2 0])))))

(defn stego-read
  ([this]
   (let [{:keys [bytes-per-byte raster width pixel done?]} @(.state this)
         [x y] (index->coords pixel width)
         output-array (int-array 4)]
     (if done?
       -1
       (do
         (dosync (alter (.state this) update-in [:pixel] inc))
         (let [result (assemble-byte (.getPixel raster x y output-array))]
           (if (= result 0)
             (do
               (dosync (alter (.state this) assoc :done? true))
               -1)
             result))))))
  ([this b off len]
   (.readSuper this b off len)))

(defn stego-skip
  [this n]
  (dosync (alter (.state this)) update-in [:pixel] + n))

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
