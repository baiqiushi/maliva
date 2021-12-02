date
echo "run_prepare_sample_tables.sh"
./run_prepare_sample_tables.sh

date
echo "run_generate_queries.sh"
./run_generate_queries.sh

date
echo "run_label_queries.sh"
./run_label_queries.sh

date
echo "run_label_sel_queries.sh"
./run_label_sel_queries.sh

date
echo "run_collect_queries_sels.sh"
./run_collect_queries_sels.sh

date
echo "run_output_sel_queries_costs.sh"
./run_output_sel_queries_costs.sh

date