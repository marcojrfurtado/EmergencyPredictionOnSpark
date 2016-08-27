#include <math_constants.h>


#define RADIUS_IN_KM 6372.8


extern "C"
// Computes the haversine distance betwwen two points on Earth
__global__ void haversine(int *size, double *in, double *out) {
    const int ix = threadIdx.x + blockIdx.x * blockDim.x;

    if (ix < *size ) {
	    const int lat1ix = 4*ix,lon1ix = (4*ix)+1,lat2ix = (4*ix)+2, lon2ix = (4*ix)+3;
	    const double dLat = (in[lat2ix] - in[lat1ix] ) * (CUDART_PI_F /180.0);
	    const double dLon = (in[lon2ix] - in[lon1ix] ) * (CUDART_PI_F /180.0);
	    const double a = pow(sin(dLat/2.0),2.0) + pow(sin(dLon/2.0),2.0) * cos(in[lat1ix] * (CUDART_PI_F/180.0)) * cos(in[lat2ix] * (CUDART_PI_F/180.0));
	    const double c = 2.0 * asin(sqrt(a));
    	    out[ix] = RADIUS_IN_KM * c;
    }
}

