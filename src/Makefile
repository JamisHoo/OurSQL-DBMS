HEADERS = db_buffer.h db_error.h db_file.h db_interface.h db_query.h  \
		  db_tablemanager.h db_common.h db_fields.h db_indexmanager.h \
		  db_outputer.h db_query_analyser.h 

SOURCE  = oursql.cc

STD11FLAGS = -std=c++11

BOOSTFLAGS = -lboost_system -lboost_filesystem

oursql: $(SOURCE) $(HEADERS)
	$(CXX) $(CXXFLAGS) $(STD11FLAGS) $(BOOSTFLAGS) $< -O2 -o $@
