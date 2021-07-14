date
echo "run_prepare_sample_tables_nyc.sh"
./run_prepare_sample_tables_nyc.sh

date
echo "run_generate_queries_nyc.sh"
./run_generate_queries_nyc.sh

date
echo "run_label_queries_nyc.sh"
./run_label_queries_nyc.sh

date
echo "run_label_sel_queries_nyc.sh"
./run_label_sel_queries_nyc.sh

date
echo "run_collect_sels_nyc.sh"
./run_collect_sels_nyc.sh

date
echo "run_output_sel_queries_costs_nyc.sh"
./run_output_sel_queries_costs_nyc.sh

date