package advanced.customwritable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

// Java Bean, todo writable precisa ser um feijãozinho do java
// 1- atributos devem ser PRIVADOS
// 2- construtor vazio
// 3- gets e sets para todos os atributos


public class FireAvgTempWritable implements WritableComparable<FireAvgTempWritable> {

    private String pais;
    private int ocorrencia;
    private String commodity;
    private String flow;


    public FireAvgTempWritable() {
    }

    public FireAvgTempWritable(String pais, int ocorrencia) {
        this.pais = pais;
        this.ocorrencia = ocorrencia;
    }

    public String getPais() {
        return pais;
    }

    public void setPais(String pais) {
        this.pais = pais;
    }

    public int getOcorrencia() {
        return ocorrencia;
    }

    public void setOcorrencia(int ocorrencia) {
        this.ocorrencia = ocorrencia;
    }

    public String getCommodity() {
        return commodity;
    }

    public void setCommodity(String commodity) {
        this.commodity = commodity;
    }

    public String getFlow() {
        return flow;
    }

    public void setFlow(String flow) {
        this.flow = flow;
    }

    @Override
    // compareTo compara se um dado vem antes do outro ou se são iguais
    // o codigo do compareTo é sempre o mesmo
    public int compareTo(FireAvgTempWritable o) {
        if(this.hashCode() < o.hashCode()) {
            return -1;
        } else if(this.hashCode() > o.hashCode()){
            return +1;
        }
        return 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ocorrencia, pais);
    }

    //tomar cuidado com a ordem em que os atributos são escritos e lidos, a ordem deve ser mantida
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(ocorrencia); //enviando a ocorrencia do objeto
        dataOutput.writeUTF(pais); //enviando o pais
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        ocorrencia = dataInput.readInt();
        pais = dataInput.readUTF();

    }
}
