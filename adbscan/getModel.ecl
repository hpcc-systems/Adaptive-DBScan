IMPORT ML_Core;
IMPORT ML_Core.Analysis;
IMPORT ML_Core.Types AS Types;
IMPORT $ AS ADBSCAN;

//performs standardization , grid search and calls adbscan
EXPORT getModel(DATASET(Types.NumericField) Records) := FUNCTION

//function to standardize the datatset
STREAMED DATASET(Types.NumericField) standardize( STREAMED DATASET(Types.NumericField) dsIn,UNSIGNED4 num) := EMBED(C++: activity) 
		#include<iostream>
    #include<bits/stdc++.h>

    using namespace std;
		
		struct rec
		{
			uint16_t wi;
			uint64_t id;
			uint32_t no;
			double val;
		};
		
		void average(vector<rec > ds, double avg[], uint32_t dim)
		{
				double sum[dim + 1] = {0.0};
	
					for(uint32_t i = 0; i < ds.size(); i++)
					{
							sum[ds[i].no] += ds[i].val;
					}
					
					for(uint32_t i = 1; i < dim + 1; i++)
					{
						avg[i] = sum[i]/(ds.size()/dim);
					}
				
		
		}
		
		void deviation(vector<rec > ds, double dev[], uint32_t dim)
		{
				double sum[dim+1] ,mean[dim +1] ,std[dim+1] ;
				memset(sum, 0, sizeof(double) * (dim + 1));
				memset(mean, 0, sizeof(double) * (dim + 1));
				memset(std, 0, sizeof(double) * (dim + 1));
				for(uint32_t i = 0; i < ds.size(); i++)
				{
							sum[ds[i].no] += ds[i].val;
				}
				
				for(uint32_t i = 1; i < dim + 1; i++)
				{
						mean[i] = sum[i]/(ds.size()/dim);
				}
				
				for(uint32_t i = 0; i < ds.size(); i++)
				{
					std[ds[i].no] += pow(ds[i].val - mean[ds[i].no], 2);
				}
				
				for(uint32_t i = 1; i < dim + 1; i++)
				{
						std[i] = sqrt(std[i]/(ds.size()/dim));
				}
				
				for(uint32_t i = 1; i < dim + 1; i++)
				{
						dev[i] = std[i];
				}
				
		}
		
		#body
	
	class ResultStream : public RtlCInterface, implements IRowStream {
    public:
        ResultStream(IEngineRowAllocator *_ra, IRowStream *_ds, uint32_t dim) : ra(_ra), ds(_ds){

     count = 0;
     for(;;)
		{
			const byte *next = (const byte *)ds->nextRow();
			if(!next)
      break;
			const byte *p = next;
			rec temp;
			
			
			
        temp.wi = *((uint16_t*)p); p += sizeof(uint16_t);
				temp.id = *((uint64_t*)p); p += sizeof(uint64_t);
				temp.no = *((uint32_t*)p); p += sizeof(uint32_t);
				temp.val = *((double*)p); p += sizeof(double);
				

			 dataset.push_back(temp);
			 
			 	rtlReleaseRow(next);	
			
				}
				double avg[dim + 1];
				double std[dim + 1];

					average(dataset,avg,dim);
					deviation(dataset,std, dim);

            for(uint32_t i = 0;i < dataset.size(); i++)
					{
					
						dataset[i].val = (dataset[i].val - avg[dataset[i].no])/std[dataset[i].no];
				
					}
					
        }
				
				
				RTLIMPLEMENT_IINTERFACE
        virtual const void* nextRow() override {
				
				if (count >= dataset.size())
            return NULL;
						
				RtlDynamicRowBuilder rowBuilder(ra);
        unsigned len = sizeof(double) + sizeof(uint16_t) + sizeof(uint64_t) + sizeof(uint32_t);
        byte * row = (byte *)rowBuilder.ensureCapacity(len, NULL);
					*((uint16_t*)row) = dataset[count].wi; 
					row += sizeof(uint16_t);
					
					*((uint64_t*)row) = dataset[count].id; 
					row += sizeof(uint64_t);
					
					*((uint32_t*)row) = dataset[count].no; 
					row += sizeof(uint32_t);
					
					*((double*)row) = dataset[count].val; 
					//row += sizeof(double);
					
					
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
        vector<rec > dataset;
        IRowStream *ds;
    };
				
				return new ResultStream(_resultAllocator, dsin, num);

ENDEMBED;

dimension := max(Records, Records.number);
ds := standardize(Records, dimension);





rs :={Types.t_FieldReal x, Types.t_FieldReal y};
rs1 :={Real x};


//possible values for threshold
poss := dataset([{0.1},{0.15},{0.2},{0.25},{0.30},{0.35},{0.4},{0.45},{0.5},{0.55},{0.6},{0.65},{0.7},{0.75},{0.8},{0.85},{0.9},{0.95}] ,rs1);



rs T1(RS1 L,integer c) := TRANSFORM
mod := ADBSCAN.ADBSCAN(l.x).Fit(ds);
test := Analysis.Clustering.SampleSilhouetteScore(ds,mod);
num := max(mod,mod.label);
self.x := if(num > 1, ave(test,value), 0);
self.y := c;
END;

//list of silhouette scores for various thresholds
Silhouettes := project(poss,T1(LEFT,counter));



maximum := max(Silhouettes, Silhouettes.x); 
ind := Silhouettes(x = maximum)[1].y;
thre := poss[ind].x ;

mod := ADBSCAN.ADBSCAN(thre).Fit(ds);

return mod;
END;
