;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.
;;
;; Copyright (c) KALEIDOS INC

(ns app.tasks.file-checkpoint
  "A maintenance task that is responsible of performing a checkpoint on a recently edited file.

  The checkpoint process right now consist on the following task:

  - Check all file-media-object references and create new if some of
    them are missing.

  "
  (:require
   [app.common.exceptions :as ex]
   [app.common.files.validate :as cfv]
   [app.common.logging :as l]
   [app.common.logic.shapes :as cls]
   [app.common.uuid :as uuid]
   [app.db :as db]
   [app.features.fdata :as feat.fdata]
   [app.storage :as sto]
   [app.tasks.file-gc :as fgc]
   [app.util.blob :as blob]
   [integrant.core :as ig]))

(def ^:dynamic *stats*
  "A test specific dynamic var for collect statistics of different
  operations performed in this task"
  nil)

(def ^:private conj*
  (fnil conj #{}))

(def ^:private sql:get-missing-media-references
  "SELECT fmo.*
     FROM file_media_object AS fmo
    WHERE fmo.id = ANY(?::uuid[])
      AND file_id != ?")

(defn- fix-media-references
  [{:keys [::db/conn] :as cfg} {file-id :id :as file}]
  (let [used-refs
        (into #{} fgc/xf:collect-used-media (cons file nil))

        missing-refs-index
        (reduce (fn [result {:keys [id] :as fmo}]
                  (assoc result id
                         (-> fmo
                             (assoc :id (uuid/next))
                             (assoc :file-id file-id)
                             (dissoc :created-at)
                             (dissoc :deleted-at))))
                {}
                (db/plan conn [sql:get-missing-media-references
                               (db/create-array conn "uuid" used-refs)
                               file-id]))

        lookup-index
        (fn [id]
          (if-let [mobj (get missing-refs-index id)]
            (do
              (l/trc :hint "lookup index"
                     :file-id (str file-id)
                     :snap-id (str (:snapshot-id file))
                     :id (str id)
                     :result (str (get mobj :id)))
              (some-> *stats* (swap! update :lookups conj* id))
              (get mobj :id))

            id))

        file
        (if (seq missing-refs-index)
          (update file :data cls/relink-shapes lookup-index)
          file)]

    (doseq [[old-id item] missing-refs-index]
      (some-> *stats* (swap! update :missing assoc old-id item))

      (l/dbg :hint "create missing references"
             :file-id (str file-id)
             :snap-id (str (:snapshot-id file))
             :old-id (str old-id)
             :id (str (:id item)))
      (db/insert! conn :file-media-object item
                  {::db/return-keys false}))

    file))

(defn- process-file-snapshots!
  [{:keys [::fgc/file-id ::db/conn ::sto/storage] :as cfg}]
  (run! (fn [{:keys [snapshot-id] :as file}]
          (let [file  (fgc/decode-file cfg file)
                file' (fix-media-references cfg file)]
            (when (not= file file')
              (cfv/validate-file-schema! file')
              (let [data (blob/encode (:data file'))]
                (some-> *stats* (swap! update :snapshots conj* snapshot-id))

                ;; If file was already offloaded, we touch the underlying storage
                ;; object for properly trigger storage-gc-touched task
                (when (feat.fdata/offloaded? file)
                  (some->> (:data-ref-id file) (sto/touch-object! storage)))

                (db/update! conn :file-change
                            {:data data
                             :data-backend nil
                             :data-ref-id nil}
                            {:id snapshot-id}
                            {::db/return-keys false})))))

        (db/plan cfg [fgc/sql:get-snapshots file-id])))

(defn- process-file!
  [cfg file-id]
  (if-let [file (fgc/get-file cfg file-id)]
    (do
      (->> file
           (fgc/decode-file cfg)
           (fix-media-references cfg)
           (cfv/validate-file-schema!)
           (fgc/persist-file! cfg))

      (process-file-snapshots! cfg)

      true)

    (do
      (l/dbg :hint "skip" :file-id (str (::fgc/file-id cfg)))
      false)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; HANDLER
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmethod ig/assert-key ::handler
  [_ params]
  (assert (db/pool? (::db/pool params)) "expected a valid database pool")
  (assert (sto/valid-storage? (::sto/storage params)) "expected valid storage to be provided"))

(defmethod ig/init-key ::handler
  [_ cfg]
  (fn [{:keys [props] :as task}]
    (let [cfg     (assoc cfg ::db/rollback (:rollback? props))
          file-id (get props :file-id)]

      (when-not (uuid? file-id)
        (ex/raise :type :internal
                  :code :invalid-props
                  :hint "file is missing or it is not uuid instance"
                  :file-id file-id))

      (try
        (db/tx-run! cfg (fn [{:keys [::db/conn] :as cfg}]
                          (let [cfg (update cfg ::sto/storage sto/configure conn)]
                            (process-file! cfg file-id))))

        (catch Throwable cause
          (l/err :hint "error file checkpoint task"
                 :file-id (str file-id)
                 :cause cause))))))
