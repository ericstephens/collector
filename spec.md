## My rules
1. Python
   1. The project uses conda for python environments so all package installations need to be done with that. The name of the environment is the same name as the project name / root folder
2. Structure
   3. Test folders should exist for src/frontend and src/api and src/db
   4. Create and modify code within the src folder and its children
   5. Avoid circular dependencies; the db should not import from the api, and the api should not import from the db
   6. Use relative imports instead of absolute imports
   7. The frontend shall not access the database directly; only access is through the API
3. DB
   1. Use PostgreSQL only for persistence. Do not degrade to SQLite
   2. Run PostgreSQL in a container
4. API
   1. Generate Python unit tests for validating the API
   2. Use pytest for Python unit tests
5. Frontend
   1. If there is a way to create unit tests for the frontend UI, do so
6. Management
   1. All stop, start, restart operations should take place with the manage.sh script
   2. When I ask for changes, focus on only those changes. I want to do small changes and small commits
7. Podman
   1. Use podman instead of docker
   2. Use podman-compose instead of docker-compose
   3. Use podman machine instead of docker machine
   4. Use podman machine ssh instead of docker machine ssh
   5. Use podman machine start instead of docker machine start
   6. Use podman machine stop instead of docker machine stop
   7. Use podman machine restart instead of docker machine restart
   8. Use podman machine rm instead of docker machine rm
   9. Use podman machine inspect instead of docker machine inspect
   10. Use podman machine list instead of docker machine list    
8.  No mocking. All real


