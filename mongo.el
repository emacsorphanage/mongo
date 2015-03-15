;;; mongo.el --- MongoDB driver for Emacs Lisp

;; Copyright (C) 2011-2015  Tomohiro Matsuyama

;; Author: Tomohiro Matsuyama <m2ym.pub@gmail.com>
;; Keywords: convenience

;; This program is free software; you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published by
;; the Free Software Foundation, either version 3 of the License, or
;; (at your option) any later version.

;; This program is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU General Public License for more details.

;; You should have received a copy of the GNU General Public License
;; along with this program.  If not, see <http://www.gnu.org/licenses/>.

;;; Commentary:

;; 

;;; Code:

(require 'cl)
(require 'bson)

(defmacro mongo-with-gensyms (names &rest body)
  (declare (indent 1))
  `(let ,(loop for name in names
               collect `(,name (gensym ,(symbol-name name))))
     ,@body))

(defsubst mongo-make-keyword (string)
  (intern (format ":%s" string)))

(defsubst mongo-generate-new-unibyte-buffer (name)
  (let ((buffer (generate-new-buffer name)))
    (with-current-buffer buffer (set-buffer-multibyte nil))
    buffer))

(defmacro* mongo-wait-for (form &key timeout (interval 0.1))
  (mongo-with-gensyms (timeout! interval! elapsed last-value)
    `(let ((,timeout! ,timeout)
           (,interval! ,interval)
           (,elapsed 0.0)
           ,last-value)
       (while (null (setq ,last-value ,form))
         (when (and ,timeout! (> ,elapsed ,timeout!))
           (error "timeout: %s" ',form))
         (sit-for ,interval!)
         (incf ,elapsed ,interval!))
       ,last-value)))



(defsubst mongo-document-oid (document)
  (bson-document-get document "_id"))



(defsubst mongo-serialize-function (name)
  (get name 'mongo-serialize-function))

(defsubst mongo-deserialize-function (name)
  (get name 'mongo-deserialize-function))

(defmacro mongo-define-serialize-function (name lambda-list &rest body)
  (declare (indent 2))
  `(put ',name 'mongo-serialize-function (lambda ,lambda-list ,@body)))

(defmacro mongo-define-deserialize-function (name lambda-list &rest body)
  (declare (indent 2))
  `(put ',name 'mongo-deserialize-function (lambda ,lambda-list ,@body)))

(defsubst mongo-serialize-of-type (object type)
  (case type
    (cstring   (bson-serialize-cstring object))
    (document  (bson-serialize-document object))
    (int32     (bson-serialize-int32 object))
    (int64     (bson-serialize-int64 object))
    (otherwise (funcall (mongo-serialize-function type) object))))

(defsubst mongo-deserialize-of-type (type &optional bound)
  (case type
    (cstring   (bson-deserialize-cstring))
    (document  (bson-deserialize-document))
    (int32     (bson-deserialize-int32))
    (int64     (bson-deserialize-int64))
    (otherwise (funcall (mongo-deserialize-function type) bound))))

(defmacro mongo-define-message-fragment (name &rest slots)
  (declare (indent 1))
  (flet
      ((make-slot-serializer (value slot-type)
         (if (consp slot-type)
             (ecase (first slot-type)
               (*
                `(loop for element in ,value
                    collect
                      (mongo-serialize-of-type
                       element ',(second slot-type)))))
             `(mongo-serialize-of-type ,value ',slot-type)))
       (make-slot-deserializer (slot-type bound)
         (if (consp slot-type)
             (ecase (first slot-type)
               (* `(loop while (< (point) ,bound)
                      collect
                        (mongo-deserialize-of-type
                         ',(second slot-type)))))
             `(mongo-deserialize-of-type ',slot-type))))
    (let ((constructor-name (intern (format "make-%s" name))))
      `(progn
         (defstruct (,name (:constructor ,constructor-name))
           ,@(loop for slot in slots collect (first slot)))
         (mongo-define-serialize-function ,name (object)
           ,@(loop for (slot-name . slot-options) in slots
                   for slot-type = (getf slot-options :type)
                   for reader = (intern (format "%s-%s" name slot-name))
                   unless (getf slot-options :transient)
                   collect (make-slot-serializer `(,reader object) slot-type)))
         (mongo-define-deserialize-function ,name (bound)
           (,constructor-name
            ,@(loop for (slot-name . slot-options) in slots
                    for slot-type = (getf slot-options :type)
                    unless (getf slot-options :transient)
                    collect (mongo-make-keyword slot-name)
                    and collect (make-slot-deserializer slot-type 'bound))))))))



(defconst mongo-op-code-table
  '((   1 . mongo-message-reply)
    (1000 . mongo-message-message)
    (2001 . mongo-message-update)
    (2002 . mongo-message-insert)
    (2004 . mongo-message-query)
    (2005 . mongo-message-get-more)
    (2006 . mongo-message-delete)
    (2007 . mongo-message-kill-cursors)))

(mongo-define-message-fragment mongo-message-header
  (message-length :type int32)
  (request-id     :type int32)
  (response-to    :type int32)
  (op-code        :type int32))

(defmacro mongo-define-message (name &rest slots)
  (declare (indent 1))
  `(mongo-define-message-fragment ,name
     (header :type mongo-message-header :transient t)
     ,@slots))

(mongo-define-message mongo-message-update
  (zero                 :type int32)
  (full-collection-name :type cstring)
  (flags                :type int32)
  (selector             :type document)
  (update               :type document))

(mongo-define-message mongo-message-insert
  (flags                :type int32)
  (full-collection-name :type cstring)
  (documents            :type (* document)))

(mongo-define-message mongo-message-query
  (flags                 :type int32)
  (full-collection-name  :type cstring)
  (number-to-skip        :type int32)
  (number-to-return      :type int32)
  (query                 :type document)
  (return-field-selector :type document))

(mongo-define-message mongo-message-get-more
  (zero                 :type int32)
  (full-collection-name :type cstring)
  (number-to-return     :type int32)
  (cursor-id            :type int64))

(mongo-define-message mongo-message-delete
  (zero                 :type int32)
  (full-collection-name :type cstring)
  (flags                :type int32)
  (selector             :type document))

(mongo-define-message mongo-message-kill-cursors
  (zero                 :type int32)
  (number-to-cursor-ids :type int32)
  (cursor-ids           :type (* int64)))

(mongo-define-message mongo-message-message
  (message :type cstring))

(mongo-define-message mongo-message-reply
  (response-flags  :type int32)
  (cursor-id       :type int64)
  (starting-from   :type int32)
  (number-returned :type int32)
  (documents       :type (* document)))

(defsubst mongo-message-header (message)
  (aref message 1))

(defsetf mongo-message-header (message) (header)
  `(aset ,message 1 ,header))

(defun mongo-serialize-message (message)
  (let* ((type (bson-type-of message))
         (op-code (car (rassq type mongo-op-code-table)))
         (start (point)))
    (assert (integerp op-code))
    (mongo-serialize-of-type message type)
    (let ((header (mongo-message-header message))
          (message-length (+ (- (point) start) 16)))
      (setf (mongo-message-header-message-length header) message-length
            (mongo-message-header-op-code header) op-code)
      (save-excursion
        (goto-char start)
        (mongo-serialize-of-type header 'mongo-message-header)))))

(defun* mongo-serialize-message-to-buffer (message
                                           &optional (buffer (current-buffer)))
  (with-current-buffer buffer
    (mongo-serialize-message message)))

(defun mongo-serialize-message-to-string (message)
  (bson-with-temp-unibyte-buffer
    (mongo-serialize-message message)
    (buffer-string)))

(defun mongo-serialize-message-to-process (message process)
  (process-send-string process (mongo-serialize-message-to-string message)))

(defun mongo-deserialize-message ()
  (let* ((header (mongo-deserialize-of-type 'mongo-message-header))
         (op-code (mongo-message-header-op-code header))
         (message-length (mongo-message-header-message-length header))
         (type (cdr (assq op-code mongo-op-code-table)))
         (bound (+ (point) message-length -16))
         (message (mongo-deserialize-of-type type bound)))
    (setf (mongo-message-header message) header)
    message))

(defun* mongo-deserialize-message-from-buffer ((buffer (current-buffer)))
  (with-current-buffer buffer (mongo-deserialize-message)))

(defun mongo-deserialize-message-from-string (string)
  (bson-with-temp-unibyte-buffer
    (insert string)
    (goto-char (point-min))
    (mongo-deserialize-message)))



(defmacro mongo-define-process-struct (name &rest slots)
  (declare (indent 1))
  (let* ((constructor-name (intern (format "make-%s" name)))
         (slot-names (loop for slot in slots
                           for slot-name = (if (listp slot) (first slot) slot)
                           collect slot-name)))
    `(progn
       (defun* ,constructor-name (underlying-process &key ,@slots)
         ,@(loop for slot-name in slot-names
                 collect `(process-put
                           underlying-process ',slot-name ,slot-name))
         underlying-process)
       ,@(loop for slot-name in slot-names
               for accessor-name = (intern (format "%s-%s" name slot-name))
               collect `(defsubst ,accessor-name (object)
                          (process-get object ',slot-name))
               collect `(defsetf ,accessor-name (object) (value)
                          `(prog1 ,value (process-put
                                          ,object ',',slot-name ,value)))))))

(mongo-define-process-struct mongo-database
  request response timeout (request-counter 0) callback)

(defvar mongo-database nil)

(defsubst mongo-peek-message-length ()
  (save-excursion (bson-deserialize-int32)))

(defun mongo-database-process-sentinel (database event))

(defun mongo-database-process-filter (database string)
  (with-current-buffer (process-buffer database)
    (goto-char (point-max))
    (insert string)
    (let ((available (buffer-size)))
      (when (>= available 4)
        (goto-char (point-min))
        (let ((message-length (mongo-peek-message-length)))
          (when (>= available message-length)
            (let ((message (mongo-deserialize-message)))
              (delete-region (point-min) (point))
              (mongo-database-process-callback database message))))))))

(defun mongo-database-process-callback (database response)
  (setf (mongo-database-response database) response)
  (bson-awhen (mongo-database-callback database)
    (funcall it database response)))

(defun* mongo-open-database (&key (host 'local)
                                  (port 27017)
                                  (make-default t)
                                  timeout
                                  callback)
  (let* ((process
          (make-network-process
           :name "mongo"
           :buffer (mongo-generate-new-unibyte-buffer " mongo")
           :host host
           :service (number-to-string port)
           :coding 'binary
           :filter 'mongo-database-process-filter
           :filter-multibyte nil
           :sentinel 'mongo-database-process-sentinel))
         (database (make-mongo-database process :callback callback)))
    (when make-default (setq mongo-database database))
    database))

(defun* mongo-close-database (&key (database mongo-database))
  (process-send-eof database))

(defmacro mongo-with-current-database (database &rest body)
  (declare (indent 1))
  `(let ((mongo-database ,database)) ,@body))

(defmacro* mongo-with-open-database ((var &rest args) &rest body)
  "Bind VAR to a db opened with ARGS and evaluate BODY.

For ARGS see `mongo-open-database'."
  (declare
   (debug (sexp &rest form))
   (indent 1))
  `(let* ((mongo-database mongo-database)
          (,var (mongo-open-database ,@args)))
     (unwind-protect
         (progn ,@body)
       (mongo-close-database :database ,var))))



(defsubst mongo-new-request-id (database)
  (incf (mongo-database-request-counter database)))

(defun mongo-finalize-request (request database)
  (let ((header (mongo-message-header request)))
    (unless header
      (setq header (make-mongo-message-header))
      (setf (mongo-message-header request) header))
    (unless (mongo-message-header-request-id header)
      (setf (mongo-message-header-request-id header)
            (mongo-new-request-id database)))
    (unless (mongo-message-header-response-to header)
      (setf (mongo-message-header-response-to header) 0))))

(defun* mongo-send-request (request &key (database mongo-database))
  (setf (mongo-database-request database) request
        (mongo-database-response database) nil)
  (mongo-finalize-request request database)
  (mongo-serialize-message-to-process request database))

(defun* mongo-receive-response (&key (database mongo-database))
  (mongo-wait-for (mongo-database-response database)
                  :timeout (mongo-database-timeout database)))

(defun* mongo-do-request (request &key (database mongo-database) async)
  (mongo-send-request request :database database)
  (unless async
    (mongo-receive-response :database database)))

(provide 'mongo)
;;; mongo.el ends here
