package org.vanilladb.core.sql.distfn;

import org.vanilladb.core.sql.Int8VectorConstant;
import org.vanilladb.core.sql.VectorConstant;

public class IntEuclideanFn extends IntDistanceFn {

    public IntEuclideanFn(String fld) {
        super(fld);
    }

    @Override
    protected int calculateDistance(Int8VectorConstant vec) {
        int sum = 0;
        for (int i = 0; i < vec.dimension(); i++) {
            int diff = query.get(i) - vec.get(i);
            sum += diff * diff;
        }
        return sum;
    }
    @Override
    protected double calculateDistance(VectorConstant vec) {
        double sum = 0;
        for (int i = 0; i < vec.dimension(); i++) {
            double diff = query.get(i) - (float)vec.get(i);
            sum += diff * diff;
        }
        return sum;
    }
}
