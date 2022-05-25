FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

ARG WORKDIR=/dataflow/template
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

ENV JSON_NAME=""

ENV PROJECT_ID=''
ENV DATASET=''
ENV TABLE_ID=''
ENV BUCKET=''

COPY . .

ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/beam_example.py"

RUN pip install pandas
RUN pip install python-dotenv
RUN pip install apache-beam[gcp]
