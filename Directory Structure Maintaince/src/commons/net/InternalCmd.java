package commons.net;

/**
 * Created by Yongtao on 8/28/2015.
 *
 * Internal command to communicate within IOControl
 * Add your control code into IOControl$Coordinator and more command types here
 */
public class InternalCmd{
	private Object attachment=null;
	private CMD cmd=null;

	public InternalCmd(){
	}

	public InternalCmd(final CMD cmd){
		this.cmd=cmd;
	}

	public InternalCmd(final CMD cmd,final Object obj){
		this.cmd=cmd;
		attachment=obj;
	}

	public CMD getCMD(){
		return cmd;
	}

	public void setCMD(final CMD cmd){
		this.cmd=cmd;
	}

	public Object getAttachment(){
		return attachment;
	}

	public void setAttachment(final Object obj){
		attachment=obj;
	}

	public enum CMD{
		EXIT,OK
	}
}
