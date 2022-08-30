(ns squery-mongo-core.roles
  (:require [squery-mongo-core.internal.convert.common :refer [single-maps]]
            [squery-mongo-core.internal.convert.commands :refer [squery-map->mql-map]]
            [squery-mongo-core.utils :refer [keyword-map]]))

(def create-role-def
  {
   ;;used to create custom roles,when the built-ins are not enough
   :createRole "role-name-string"

   ;;resource
   ;;  for example  {resource : {db: "joy" collection "users"} actions: [ "find", "update", "insert", "remove" ] }
   ;;  for example  {resource: {cluster:true} , actions: ["shutdown"]}
   ;;  empty string(on collection or db) means ALL for example {db: "joy" , collections: ""} means all collections in joy

   ;;actions can be many like  readWrite or Shutdown (the cluster)
   :privileges [
                { :resource "<resource_document>", :actions [ "<action1>" "<action2>" "..." ] }

                ]


   :roles [
           { :role "<role>", :db "<database>" }
           "<role>   (db will assumed the db that i run the command)"

           ]

   :authenticationRestrictions [
                                {
                                 :clientSource ["<IP>|<CIDR range>" "...."]
                                 :serverAddress ["<IP>|<CIDR range>" "...."]
                                 }
                                "..."
                                ]
   :writeConcern "document"
   })

(defn create-update-role [db-name command-map update? args]
  (let [command-map (keyword-map command-map)
        role-name (if update?
                    (get command-map :updateRole)
                    (get command-map :createRole))

        squery-map (dissoc :updateRole :createRole)

        command-body (squery-map->mql-map squery-map)
        command-head (if update? {"updateRole" role-name} {"createRole" role-name})

        ;- (clojure.pprint/pprint command-map)
        ]
    {:db db-name
     :coll nil
     :command-head command-head
     :command-body command-body}))


(defn create-role
  "Roles like users belong to a database,admin database is better to be used to know where all info is
  Requires = createRole/grantRole actions. Built in role 'userAdmin' can do those actions
             setAuthenticationRestriction action"
  [db-name command-map & args]
  (create-update-role db-name command-map false args))

;;You must have access that includes the revokeRole action on all databases in order to update a user’s roles array.
;
;You must have the grantRole action on a role’s database to add a role to a user.
;
;To change another user’s pwd or customData field, you must have the changeAnyPassword and changeAnyCustomData actions respectively on that user’s database.
;
;To modify your own password and custom data, you must have privileges that grant changeOwnPassword and changeOwnCustomData actions respectively on the user’s database.
(defn update-role
  "Requires =
  revokeRole action on all databases in order to update a role
  grantRole action on the database of each role in the roles array to update the array
  grantRole action on the database of each privilege in the privileges array to update the array
  If a privilege’s resource spans databases, you must have grantRole on the admin database.
  A privilege spans databases if the privilege is any of the following
    a collection in all database
    all collections and all databas
    the cluster resource
  setAuthenticationRestriction action on the database of the target role if change :authenticationRestrictions

  A top level field that is present in the update command => completly replace the old one
  its like invoke first and grant after\n   For not fully replace use  grant/revoke commands"
  [db-name command-map & args]
  (create-update-role db-name command-map true args))


(defn drop-role
  "Requires = dropRole action"
  [db-name rolename & args]
  (let [options-map (apply (partial merge {})args)
        squery-map options-map

        command-head {"dropRole" rolename}
        command-body (squery-map->mql-map squery-map)]
    {:db db-name
     :coll nil
     :command-head command-head
     :command-body command-body}))


(defn drop-all-roles-from-database
  "Removes all roles from the argument db(users that belong to the database)
  Requires = dropRole action"
  [db-name & args]
  (let [
        options-map (apply (partial merge {})args)
        squery-map options-map

        command-head {"dropAllRolesFromDatabase" 1}
        command-body (squery-map->mql-map squery-map)]
    {:db db-name
     :coll nil
     :command-head command-head
     :command-body command-body}))

(defn grant-privileges-to-role [db-name rolename privileges & args]
  (let [
        options-map (apply (partial merge {})args)
        squery-map (merge {:privileges privileges} options-map)

        command-head {"grantPrivilegesToRole" rolename}
        command-body (squery-map->mql-map squery-map)]
    {:db db-name
     :coll nil
     :command-head command-head
     :command-body command-body}))


(defn grant-roles-to-role [db-name rolename roles & args]
  (let [options-map (apply (partial merge {})args)
        squery-map (merge {:roles roles} options-map)

        command-head {"grantRolesToRole" rolename}
        command-body (squery-map->mql-map squery-map)]
    {:db db-name
     :coll nil
     :command-head command-head
     :command-body command-body}))

(defn invalidate-user-cache
  "Flushes user information from in-memory cache, including removal of each user’s credentials and roles.
  This allows you to purge the cache at any given moment, regardless of the interval set in the
  userCacheInvalidationIntervalSecs parameter
  Requires = invalidateUserCache action on the cluster resource in order to use this command"
  [db-name & args]
  (let [command-head {"invalidateUserCache" 1}
        command-body {}]
    {:db db-name
     :coll nil
     :command-head command-head
     :command-body command-body}))

(defn revoke-privileges-from-role
  "The resource must be an exact much.
  Actions can be subsets"
  [db-name rolename privileges & args]
  (let [options-map (apply (partial merge {})args)
        squery-map (merge {:privileges privileges} options-map)

        command-head {"revokePrivilegesFromRole" rolename}
        command-body (squery-map->mql-map squery-map)]
    {:db db-name
     :coll nil
     :command-head command-head
     :command-body command-body}))

(defn revoke-role-from-role
  "Both built-in roles and user-defined roles
  Requires = revokeRole action"
  [db-name rolename privileges & args]
  (let [
        options-map (apply (partial merge {})args)
        squery-map (merge {:privileges privileges} options-map)

        command-head {"revokeRolesFromRole" rolename}
        command-body (squery-map->mql-map squery-map)]
    {:db db-name
     :coll nil
     :command-head command-head
     :command-body command-body}))

;;{
;  rolesInfo: { role: <name>, db: <db> },
;  showPrivileges: <Boolean>,
;  showBuiltinRoles: <Boolean>
;}

(defn roles-info
  "Command
   {
    rolesInfo: <see below>
    showPrivileges: <Boolean>
    showBuiltinRoles: <Boolean>
   }

  rolesInfo
    { rolesInfo: 1, showBuiltinRoles: true }  all from the db run command
    { rolesInfo: { role: <rolename>, db: <database> } }
    { rolesInfo: [ 'roleName' { role : <name>, db: <db> } ...] }  many
  Requires =  viewRole action to see a role you dont have
             (but each user is allowed to see their personal roles info)
  "
  [db-name command-map & args]
  (let [
        squery-map (keyword-map command-map)
        squery-map (if (contains? squery-map :filter)
                      (assoc squery-map :filter {"$expr" (get squery-map :filter)})
                      squery-map)

        roles-info-value (get command-map :rolesInfo)

        command-head {"rolesInfo" roles-info-value}
        command-body (squery-map->mql-map squery-map)]
    {:db db-name
     :coll nil
     :command-head command-head
     :command-body command-body}))