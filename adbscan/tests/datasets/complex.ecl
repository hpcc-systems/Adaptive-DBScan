EXPORT complex := MODULE
layout := RECORD
		REAL X;
		REAL Y;
	END;
EXPORT ds := DATASET('~.::book2.csv',layout,THOR);
END;
