(ns squery-mongo-core.users
  (:require [squery-mongo-core.internal.convert.common :refer [single-maps]]
            [squery-mongo-core.internal.convert.commands :refer [squery-map->mql-map]]
            [squery-mongo-core.utils :refer [keyword-map]]))

(def create-user-def
  {
   :createUser "userName-string",
   :pwd "passwordPrompt()|password" ,   ;;if passwordPrompt after the command mongo will ask for password
   :customData "<any information document> "  ;; any data of my choice
   :roles [
           { :role "<role>", :db "<database>" } ;;specify role+database (db can be different form the one i use to call the command)
           "<role>"    ;; if no :db = the database i run the command from

           ]
   :writeConcern "document"

   ;;network restrictions for example what IP can connect to that server etc
   :authenticationRestrictions [
                                { :clientSource [ "<IP|CIDR range>"  "..." ] :serverAddress [ "<IP|CIDR range>", "..." ] },
                                ],

   :mechanisms [ "<scram-mechanism>"]    ;;specify authentication mechanism,default = scram
   :digestPassword "bool"  ;;default=true => the server receives undigested password from the client and digests the password.
   })

(defn create-update-user [db-name command-map update? args]
  (let [
        squery-map (keyword-map command-map)
        user-name (if update?
                    (get squery-map :updateUser)
                    (get squery-map :createUser))
        squery-map (dissoc :updateUser :createUser)

        command-head (if update? {"updateUser" user-name} {"createUser" user-name})
        command-body (squery-map->mql-map squery-map)

        ;- (clojure.pprint/pprint command-map)
        ]
    {:db db-name
     :coll nil
     :command-head command-head
     :command-body command-body}))


(defn create-user
  "Requires = createUser action  Built in role 'userAdmin' can do those actions
              setAuthenticationRestriction action if add those also
  UserName is on the command-map,and :pwd for password"
  [db-name command-map & args]
  (create-update-user db-name command-map false args))

;;You must have access that includes the revokeRole action on all databases in order to update a user’s roles array.
;You must have the grantRole action on a role’s database to add a role to a user.
;To change another user’s pwd or customData field, you must have the changeAnyPassword and changeAnyCustomData actions respectively on that user’s database.
;To modify your own password and custom data, you must have privileges that grant changeOwnPassword and changeOwnCustomData actions respectively on the user’s database.
(defn update-user
  "Requires = revokeRole/grantRole/ action
              changeAnyPassword action if update on password(or changeOwnPassword if i update my own)
              changeAnyCustomData action if update custom data(or changeOwnCustomData)
              setAuthenticationRestriction action if change those also
  A top level field that is present in the update command => completly replace the old one,
  its like invoke first and grant after
  For not fully replace use  grant/revoke commands
  UserName is on the command-map,and :pwd for password"
  [db-name command-map & args]
  (create-update-user db-name command-map true args))

(defn drop-all-users-from-database
  "Removes all users from the argument db(users that belong to the database)
  Requires = dropUser action"
  [db-name & args]
  (let [options-map (apply (partial merge {})args)
        squery-map options-map

        command-head {"dropAllUsersFromDatabase" 1}
        command-body (squery-map->mql-map squery-map)]
    {:db db-name
     :coll nil
     :command-head command-head
     :command-body command-body}))

(defn drop-user
  "Removes a user from the db(only if user belong to the database)
   Requires = dropUser action"
  [db-name username & args]
  (let [
        options-map (apply (partial merge {})args)
        squery-map options-map

        command-head {"dropUser" username}
        command-body (squery-map->mql-map squery-map)]
    {:db db-name
     :coll nil
     :command-head command-head
     :command-body command-body}))

(defn grant-roles
  "Roles= [{ :role 'rolename', :db 'databasename' } 'role' ... ]
  if not :db ,the db is the one that the command runned from
  Requires = grantRole action"
  [db-name username roles & args]
  (let [
        options-map (apply (partial merge {})args)
        squery-map (merge {:roles roles} options-map)

        command-head {"grantRolesToUser" username}
        command-body (squery-map->mql-map squery-map)]
    {:db db-name
     :coll nil
     :command-head command-head
     :command-body command-body}))


(defn revoke-roles
  "Roles = [{ :role 'rolename', :db 'databasename' } 'rolename' ...]
  if not :db ,the db is the one that the command runned from
  The user that have this roles will lose them
  Requires =  revokeRole action"
  [db-name username roles & args]
  (let [
        options-map (apply (partial merge {})args)
        squery-map (merge {:roles roles} options-map)

        command-head {"revokeRolesFromUser" username}
        command-body (squery-map->mql-map squery-map)]
    {:db db-name
     :coll nil
     :command-head command-head
     :command-body command-body}))



(defn users-info
  "Command
   {
    usersInfo: <see below>
    showCredentials: <Boolean>
    showPrivileges: <Boolean>
    showAuthenticationRestrictions: <Boolean>
    filter: <document> (aggregation operators here only)
    }

  usersInfo
    { usersInfo: 1 } => for all users in the db
    { forAllDBs: true } => for all users in all databases

    { usersInfo: username } => for specific user on db run the command
    { usersInfo: [username1 ....] } => for specific users

    { usersInfo: { user: <name>, db: <db> } }
      => specific user but from another db not the one runs the command
    { usersInfo: [{ user: <name>, db: <db> } ...] }
      => for specific users from those databases

  Requires = viewUser action to see another users info
            (but each user is allowed to see their personal info)
  "
  [db-name command-map & args]
  (let [
        squery-map (keyword-map command-map)
        squery-map (if (contains? squery-map :filter)
                      (assoc squery-map :filter {"$expr" (get squery-map :filter)})
                      squery-map)

        users-info-value (get squery-map :usersInfo)

        command-head {"usersInfo" users-info-value}
        command-body (squery-map->mql-map squery-map)]
    {:db db-name
     :coll nil
     :command-head command-head
     :command-body command-body}))