image:
	docker build . --tag benflow-airflow-image:2.0.2

run-docker: image
	docker-compose up