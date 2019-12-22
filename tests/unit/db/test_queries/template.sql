{% for k, v in params.items()%}
SELECT * FROM pg_tables WHERE tablename = '{{v}}';{% endfor %}
