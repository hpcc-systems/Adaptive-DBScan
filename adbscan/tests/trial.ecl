IMPORT ML_Core;
IMPORT ML_Core.Analysis;
IMPORT ML_Core.Types AS Types;
IMPORT $.^ AS ADBSCAN;
IMPORT $.datasets.adap AS compound_data;
IMPORT $.datasets;

actual := $.datasets.adap_actual;
Records := compound_data.ds;
layout := compound_data.layout;

ML_Core.AppendSeqID(Records,id,dsID);
ML_Core.ToField(dsID,dsNF);

mod := ADBSCAN.getModel(dsNF);
NumberOfClusters := ADBSCAN.ADBSCAN().Num_Clusters(mod);
NumberOfOutliers := ADBSCAN.ADBSCAN().Num_Outliers(mod);
test1 := Analysis.Clustering.ARI(mod, actual);

OUTPUT(NumberOfClusters, NAMED('NumberOfClusters'));
OUTPUT(NumberOfOutliers, NAMED('NumberOfOutliers'));
output(test1, , NAMED('ARI'));