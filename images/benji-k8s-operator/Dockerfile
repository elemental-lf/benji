FROM python:3.7
ADD images/benji-k8s-operator/benji_operator.py benji_operator.py
RUN pip install kopf==0.19 kubernetes==9.0.0
CMD kopf run benji_operator.py
