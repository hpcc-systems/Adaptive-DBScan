IMPORT ML_Core;
IMPORT ML_Core.Analysis;
IMPORT ML_Core.Types AS Types;
IMPORT $ AS ADBSCAN;
IMPORT $.tests.datasets.adap AS compound_data;
IMPORT $.tests.datasets;

layout := compound_data.layout;

EXPORT GridSearch := MODULE

EXPORT stand(DATASET(layout) Records,UNSIGNED4 dimension) := FUNCTION

STREAMED DATASET(layout) standardize( STREAMED DATASET(layout) dsIn,UNSIGNED4 num) := EMBED(C++: activity) 
		#include<iostream>
    #include<bits/stdc++.h>

    using namespace std;
		
		void average(vector<vector<double> > ds, double avg[], uint32_t dim)
		{
				double sum;
				for(uint32_t j = 0;j < dim; j++)
				{
				sum = 0;
					for(uint32_t i = 0; i < ds.size(); i++)
					{
							sum += ds[i][j];
					}
					
					avg[j] = sum/ds.size();
				}
		
		}
		
		void deviation(vector<vector<double> > ds, double dev[], uint32_t dim)
		{
				double sum,mean,std;
				for(uint32_t j = 0;j < dim; j++)
				{
				sum = 0;
				std = 0;
					for(uint32_t i = 0; i < ds.size(); i++)
					{
							sum += ds[i][j];
					}
					
					mean = sum/ds.size();
					
					for(uint32_t i = 0; i < ds.size(); i++)
					{
						std += pow(ds[i][j]-mean, 2);
					}
					
					std = sqrt(std/ds.size());
					dev[j] = std;
				}
		}
		
		#body
	
	class ResultStream : public RtlCInterface, implements IRowStream {
    public:
        ResultStream(IEngineRowAllocator *_ra, IRowStream *_ds, uint32_t dim) : ra(_ra), ds(_ds){

     count = 0;
		 d = dim;
     for(;;)
		{
			const byte *next = (const byte *)ds->nextRow();
			if(!next)
      break;
			const byte *p = next;
			vector<double> temp;
			
			for(uint32_t i=0; i<dim; ++i){
       double f = *((double*)p); p += sizeof(double);
			 
			 temp.push_back(f);
       }
			 
			 
			 dataset.push_back(temp);
			 	rtlReleaseRow(next);	
			
				}
				double avg[dim];
				double std[dim];

					average(dataset,avg,dim);
					deviation(dataset,std, dim);

            for(uint32_t i = 0;i < dataset.size(); i++)
					{
						vector<double> temp;
							double t;
						for(uint32_t j = 0;j < dim; j++)
						{			
							t = (dataset[i][j] - avg[j])/std[j];
							temp.push_back(t);
						}
						results.push_back(temp);
						temp.clear();
						
					}
					
        }
				
				
				RTLIMPLEMENT_IINTERFACE
        virtual const void* nextRow() override {
				
				if (count >= results.size())
            return NULL;
						
				RtlDynamicRowBuilder rowBuilder(ra);
        unsigned len = sizeof(double) * d;
        byte * row = (byte *)rowBuilder.ensureCapacity(len, NULL);
					for(uint32_t i  = 0; i < d; i++)
					{
					*((double*)row) = results[count][i]; 
					if(i != d -1)
					row += sizeof(double);
					}
					
					count++;
				return rowBuilder.finalizeRowClear(len);
				}
				
				virtual void stop()
    {
        //count = (unsigned)-1;
    }
				
								
				 protected:
        Linked<IEngineRowAllocator> ra;
        unsigned count;
				unsigned d;
        vector<vector<double> > dataset;
        vector<vector<double> > results;
        IRowStream *ds;
    };
				
				return new ResultStream(_resultAllocator, dsin, num);

ENDEMBED;

ds := standardize(Records, dimension);

return ds;

END;

EXPORT Adaptive_Clustering(DATASET(LAYOUT) ds) := FUNCTION



rs :={Types.t_FieldReal x, Types.t_FieldReal y};
rs1 :={Real x};

ML_Core.AppendSeqID(ds,id,dsID);
ML_Core.ToField(dsID,dsNF);

poss := dataset([{0.1},{0.15},{0.2},{0.25},{0.30},{0.35},{0.4},{0.45},{0.5},{0.55},{0.6},{0.65},{0.7},{0.75},{0.8},{0.85},{0.9},{0.95}] ,rs1);



rs T1(RS1 L,integer c) := TRANSFORM
mod := ADBSCAN.ADBSCAN(l.x).Fit(dsNF);
test := Analysis.Clustering.SampleSilhouetteScore(dsNF,mod);
num := max(mod,mod.label);
self.x := if(num > 1, ave(test,value), 0);
self.y := c;
END;


Silhouettes := project(poss,T1(LEFT,counter));

maximum := max(Silhouettes, Silhouettes.x);
ind := Silhouettes(x = maximum)[1].y;
thre := poss[ind].x ;

mod := ADBSCAN.ADBSCAN(thre).Fit(dsNF);

return mod;
END;
END;