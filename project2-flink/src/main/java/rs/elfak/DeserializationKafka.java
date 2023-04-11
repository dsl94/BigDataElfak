package rs.elfak;

import com.google.gson.Gson;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class DeserializationKafka implements DeserializationSchema<OsloRide> {
    private static final Gson gson = new Gson();

    @Override
    public TypeInformation<OsloRide> getProducedType() {
        return TypeInformation.of(OsloRide.class);
    }

    @Override
    public OsloRide deserialize(byte[] arg0) throws IOException {
        OsloRide res = gson.fromJson(new String(arg0), OsloRide.class);
        return res;
    }

    @Override
    public boolean isEndOfStream(OsloRide arg0) {
        return false;
    }

}
