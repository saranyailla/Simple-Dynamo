package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.*;
import java.net.*;
import java.security.*;
import java.util.*;

import android.content.*;
import android.database.*;
import android.net.Uri;
import android.telephony.TelephonyManager;
import android.util.*;

public class SimpleDynamoProvider extends ContentProvider {

	static HashSet<String> myFiles= new HashSet<String>();
	static final int SERVER_PORT = 10000;
	static String nextPort="";
	static String nextnextPort="";
	static String prevPort="";
	static String portStr="";
	static String myPortHash="";
	static String PrevPrevPort="";
	static ArrayList<String> halfPorts= new ArrayList<String>();
	HashMap<String,String> halfHash= new HashMap<String, String>();
	static Boolean min=false;
	static ArrayList<String> ports=new ArrayList<String>();
	static HashMap<String,ArrayList<String>> portNeighbours= new HashMap<String,ArrayList<String>>();
static HashMap<String, String> replicationStatus= new HashMap<String, String>();
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		if(selection.contains("@") || selection.contains("*")){
			File[] files  = getContext().getFilesDir().listFiles();
			for(File file:files){
				file.delete();

			}
			replicationStatus=null;
		}
		else{
			if(myFiles.contains(selection)) {
			    Log.e("delete","key found: "+selection);
				File dir = getContext().getFilesDir();
				File f = new File(dir, selection);
				if (f.exists()) {
				    f.delete();

                    if(replicationStatus.containsKey(selection)) {
                        replicationStatus.remove(selection);
                       // Log.e("delete"," in replica"+selection);
                    }else{
                        if(selectionArgs==null) {
                         //   Log.e("delete"," in normal "+selection);

                            Message m1 = new Message();
                            m1.setPortProp(portNeighbours.get(myPortHash).get(1));
                            m1.setKeyValue(selection);
                            m1.setInsertionType("delete");
                            Client c1 = new Client(m1);
                            c1.start();
                            c1.deleteQuery();

                            Message m2 = new Message();
                            m2.setPortProp(portNeighbours.get(myPortHash).get(2));
                            m2.setKeyValue(selection);
                            m2.setInsertionType("delete");
                            Client c2 = new Client(m2);
                            c2.start();
                            c2.deleteQuery();
                        }
                    }
                }



			}else{
			    if(selectionArgs!=null){
			       // Log.e("delete","found me but I dont have any");
                    return 0;
                }
				try {
				  //  Log.e("delete"," testing else");
					String keyHash = genHash(selection);
					for (int i = 0; i < ports.size(); i++) {

						if ((i == ports.size() - 1 && keyHash.compareTo(ports.get(i)) > 0) || (i == 0 && keyHash.compareTo(ports.get(i)) <= 0)) {
							//Log.e("Prop", halfHash.get(ports.get(0)) + ", msg" + selection);
							Message m = new Message();
							m.setPortProp(ports.get(0));
							m.setKeyValue(selection);
							m.setInsertionType("delete");
							Client c = new Client(m);
							c.start();
							c.deleteQuery();

                            Message m1 = new Message();
                            m1.setPortProp(portNeighbours.get(ports.get(0)).get(1));
                            m1.setKeyValue(selection);
                            m1.setInsertionType("delete");
                            Client c1 = new Client(m1);
                            c1.start();
                            c1.deleteQuery();

                            Message m2 = new Message();
                            m2.setPortProp(portNeighbours.get(ports.get(0)).get(2));
                            m2.setKeyValue(selection);
                            m2.setInsertionType("delete");
                            Client c2 = new Client(m2);
                            c2.start();
                            c2.deleteQuery();


						} else if (keyHash.compareTo(ports.get(i)) > 0 && keyHash.compareTo(ports.get(i + 1)) <= 0) {

							Message m = new Message();
							m.setPortProp(ports.get(i + 1));
							m.setKeyValue(selection);
							m.setInsertionType("delete");
							Client c = new Client(m);
							c.start();
							 c.deleteQuery();


                            Message m1 = new Message();
                            m1.setPortProp(portNeighbours.get(ports.get(i+1)).get(1));
                            m1.setKeyValue(selection);
                            m1.setInsertionType("delete");
                            Client c1 = new Client(m1);
                            c1.start();
                            c1.deleteQuery();

                            Message m2 = new Message();
                            m2.setPortProp(portNeighbours.get(ports.get(i+1)).get(2));
                            m2.setKeyValue(selection);
                            m2.setInsertionType("delete");
                            Client c2 = new Client(m2);
                            c2.start();
                            c2.deleteQuery();

						} else {
							continue;

						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub

		String msg_key = values.getAsString("key");
		String msg_value = values.getAsString("value");
		String msg_type=values.getAsString("type");
		Context con = getContext();

		try {

			String keyHash = genHash(msg_key);
				if(myPortHash.compareTo(prevPort)<0 && myPortHash.compareTo(nextPort)<0 && !min){
					min=true;
				}

				if(min) {
					if ((keyHash.compareTo(prevPort) > 0 && keyHash.compareTo(myPortHash) > 0) || (keyHash.compareTo(myPortHash) < 0 && keyHash.compareTo(prevPort) < 0)) {
						//Log.e("min","Insertion"+msg_key);
						FileOutputStream out = con.openFileOutput(msg_key, Context.MODE_PRIVATE);
						out.write(msg_value.getBytes());
						out.close();
						myFiles.add(msg_key);
						if(msg_type==null) {
                           // Log.e("Fun","...");
                            Message message = new Message();
                            message.setInsertionType("replicate");
                            message.setNextPortProp(nextPort);
                            message.setNextnextPortProp(nextnextPort);
                            message.setPortProp(myPortHash);
                            message.setKeyValue(msg_key + "," + msg_value);
                            Client c = new Client(message);
                            c.start();
                        }
						return uri;
					}
				}
			 if((keyHash.compareTo(myPortHash)<=0 && keyHash.compareTo(prevPort)>0 )){

				FileOutputStream out = con.openFileOutput(msg_key, Context.MODE_PRIVATE);
				out.write(msg_value.getBytes());
				out.close();
				myFiles.add(msg_key);
                 if(msg_type==null) {

                     Message message = new Message();
                     message.setInsertionType("replicate");
                     message.setKeyValue(msg_key + "," + msg_value);
                     message.setNextPortProp(nextPort);
                     message.setNextnextPortProp(nextnextPort);
                     message.setPortProp(myPortHash);
                     Client c = new Client(message);
                     c.start();
                 }
				return uri;
			}else{
				for(int i=0;i< ports.size();i++){

					if((i==ports.size()-1 && keyHash.compareTo(ports.get(i))>0) ||(i==0 && keyHash.compareTo(ports.get(i))<=0)){
							Message m= new Message();
							m.setPortProp(ports.get(0));
							m.setKeyValue(msg_key+","+msg_value);
							m.setInsertionType("normal");
							Client c= new Client(m);
							c.start();
						Message message= new Message();
						message.setInsertionType("replicate");
						message.setKeyValue(msg_key+","+msg_value);
						message.setNextPortProp(portNeighbours.get(ports.get(0)).get(1));
						message.setNextnextPortProp(portNeighbours.get(ports.get(0)).get(2));
						message.setPortProp(ports.get(0));
						Client c1= new Client(message);
						c1.start();



					}
					else if(keyHash.compareTo(ports.get(i))>0 && keyHash.compareTo(ports.get(i+1))<=0){
						Message m= new Message();
						m.setPortProp(ports.get(i+1));
						m.setKeyValue(msg_key+","+msg_value);
						m.setInsertionType("normal");
						Client c= new Client(m);
						c.start();
						Message message= new Message();
						message.setInsertionType("replicate");
						message.setKeyValue(msg_key+","+msg_value);
						message.setNextPortProp(portNeighbours.get(ports.get(i+1)).get(1));
						message.setNextnextPortProp(portNeighbours.get(ports.get(i+1)).get(2));
						message.setPortProp(ports.get(i+1));
						Client c1= new Client(message);
						c1.start();
					}
					else{
						continue;

					}
				}
			}
			return null;
		} catch (Exception e) {
			Log.e("exception", "");
		}
		return uri;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);//5554
		prevPort=portStr;
		nextPort=portStr;
		nextnextPort=portStr;
		Server sarvam= new Server();
		sarvam.start();
		Log.e("My port",portStr);
		halfPorts.add("5554");
		halfPorts.add("5556");
		halfPorts.add("5558");
		halfPorts.add("5560");
		halfPorts.add("5562");
		try {
			myPortHash=genHash(portStr);
			for( String s:halfPorts){
				String portHash=genHash(s);
				ports.add(portHash);
				halfHash.put(portHash,s);
			}

			Collections.sort(ports);
			Log.e("..","............");
			for(int i=0;i<ports.size();i++){
				Log.e("Ports",halfHash.get(ports.get(i)));
			}
			Log.e("..","............");
			for(int i=0;i<ports.size();i++){
				//if(myPortHash.compareTo(ports.get(i))==0){
					if(i==0){
						prevPort=ports.get(ports.size()-1);
						nextPort=ports.get(i+1);
						nextnextPort=ports.get(i+2);



						ArrayList<String> as = new ArrayList<String>();
						as.add(prevPort);
						as.add(nextPort);
						as.add(nextnextPort);
                        portNeighbours.put(ports.get(i),as);

					}else if(i==ports.size()-1){
						prevPort=ports.get(i-1);
						nextPort=ports.get(0);
						nextnextPort=ports.get(1);

						ArrayList<String> as = new ArrayList<String>();
						as.add(prevPort);
						as.add(nextPort);
						as.add(nextnextPort);
                        portNeighbours.put(ports.get(i),as);

					}
					else if(i==ports.size()-2){
						prevPort=ports.get(i-1);
						nextPort=ports.get(i+1);
						nextnextPort=ports.get(0);
						ArrayList<String> as = new ArrayList<String>();
						as.add(prevPort);
						as.add(nextPort);
						as.add(nextnextPort);
						portNeighbours.put(ports.get(i),as);
					}
					else{
						prevPort=ports.get(i-1);
						nextPort=ports.get(i+1);
						nextnextPort=ports.get(i+2);
						ArrayList<String> as = new ArrayList<String>();
						as.add(prevPort);
						as.add(nextPort);
						as.add(nextnextPort);
						portNeighbours.put(ports.get(i),as);
					}
				}
			//}
        prevPort=portNeighbours.get(myPortHash).get(0);
			nextPort=portNeighbours.get(myPortHash).get(1);
			nextnextPort=portNeighbours.get(myPortHash).get(2);
			PrevPrevPort=portNeighbours.get(prevPort).get(0);
//        for(Map.Entry<String, ArrayList<String>> hm : portNeighbours.entrySet()){
//            Log.e("Oncreate",hm.getKey()+"...."+hm.getValue());
//        }
			Log.e("prevprevport:", PrevPrevPort);
			Message message= new Message();
			message.setInsertionType("repStatus");
			Client c= new Client(message);
			c.start();

		}
		catch (Exception e){

		}
		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub

		String write_fields[] = {"key", "value"};
		MatrixCursor mc = new MatrixCursor(write_fields);
		Context context_obj = getContext();
		String reqPort = portStr;

		if (selection.equals("*") || selection.equals("@")) {

			for (String s : myFiles) {
				try {
					//using FileInputStream class
					FileInputStream fis = context_obj.openFileInput(s);
					InputStreamReader inputStreamReader = new InputStreamReader(fis);
					String retrieved_values = new BufferedReader(inputStreamReader).readLine();
					String arr_content[] = {s, retrieved_values};
					mc.addRow(arr_content);
					fis.close();
					// return mc;
				} catch (IOException e) {

					//Log.e(TAG,e.getMessage());
					Log.e("Qu", "Querying failed");
				}
			}

			if(selection.equals("*")){
				for(String s:halfPorts){
					//int num=2*Integer.parseInt(portStr);
					//Log.e("ports",s);
					if(!(s).equals(portStr) ){
						Message m = new Message();
						//Log.e("Called",s);
						m.setInsertionType("all");
						m.setPortProp(s);
						Client c1= new Client(m);
						c1.start();
						ArrayList<String> ast=c1.allAns();
					//	Log.e("Size in query",ast.size()+"");
						if(ast!=null) {
							for (String b : ast) {
								String arr_content[] = {b.split(",")[0], b.split(",")[1]};
								mc.addRow(arr_content);
							}
						}
					}
				}
			}
		}
		else {
			if (myFiles.contains(selection)) {
				try {
					//using FileInputStream class
					FileInputStream fis = context_obj.openFileInput(selection);
					InputStreamReader inputStreamReader = new InputStreamReader(fis);
					String retrieved_values = new BufferedReader(inputStreamReader).readLine();
					String arr_content[] = {selection, retrieved_values};
					mc.addRow(arr_content);
					fis.close();
					return mc;
				} catch (IOException e) {
					//Log.e(TAG,e.getMessage());
					//Log.e("Qu", "Querying failed");
				}
			} else {
				try {
					String keyHash = genHash(selection);
					for (int i = 0; i < ports.size(); i++) {

						if ((i == ports.size() - 1 && keyHash.compareTo(ports.get(i)) > 0) || (i == 0 && keyHash.compareTo(ports.get(i)) <= 0)) {
						//	Log.e("Prop", halfHash.get(ports.get(0)) + ", msg" + selection);
							Message m = new Message();
							m.setPortProp(ports.get(0));
							m.setKeyValue(selection);
							m.setInsertionType("query");
							Client c = new Client(m);
							c.start();
							String answer[] = c.ans();
							if(answer==null){
							    Log.e("query","null ans");
                                Message m1 = new Message();
                                m1.setPortProp(ports.get(1));
                                m1.setKeyValue(selection);
                                m1.setInsertionType("query");
                                Client c1 = new Client(m1);
                                c1.start();
                                String answer1[]=c1.ans();
                                mc.addRow(answer1);
                            }
                            else {
                                mc.addRow(answer);

                            }
							return mc;


						} else if (keyHash.compareTo(ports.get(i)) > 0 && keyHash.compareTo(ports.get(i + 1)) <= 0) {
						//	Log.e("Prop", halfHash.get(ports.get(i + 1)) + ", msg" + selection);
							Message m = new Message();
							m.setPortProp(ports.get(i + 1));
							m.setKeyValue(selection);
							m.setInsertionType("query");
							Client c = new Client(m);
							c.start();

                            String answer[] = c.ans();
                            if(answer==null){
                                Log.e("query","null ans");
                                Message m1 = new Message();
                                m1.setPortProp(portNeighbours.get(ports.get(i+1)).get(1));
                                m1.setKeyValue(selection);
                                m1.setInsertionType("query");
                                Client c1 = new Client(m1);
                                c1.start();
                                String answer1[]=c1.ans();
                                mc.addRow(answer1);
                            }
                            else {
                                mc.addRow(answer);

                            }
							return mc;

						} else {
							continue;

						}
					}


					//}

				} catch (Exception e) {
					e.printStackTrace();
				}

			}
		}
		return mc;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
	public class Client extends Thread {
		Message msg;

		Client(Message m) {
			this.msg = m;
		}
		Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

		public void run() {
			try {
			    Log.e("client ", msg.getInsertionType()+","+msg.getKeyValue());
			    //Log.e("client msg type: ", msg.getInsertionType());
				if(msg.getInsertionType().equals("normal")) {
				    //Log.e("client",msg.getKeyValue());
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), 2 * Integer.parseInt(halfHash.get(msg.getPortProp())));

                    try {
                        DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                        dos.writeUTF(msg.getInsertionType() + "," + msg.getKeyValue());
                        DataInputStream ois1 = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
                        String ack=ois1.readUTF();
                        if(ack.equals("done")){
                            socket.close();
                        }
                    }
                    catch(Exception e){
                        socket.close();
                    }
				}else if(msg.getInsertionType().equals("replicate")){
					msg.setInsertionType("replication");
					//Log.e("client",msg.getKeyValue());
					Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), 2 * Integer.parseInt(halfHash.get(msg.getNextPortProp())));
					DataOutputStream dos1 = new DataOutputStream(socket1.getOutputStream());
					Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), 2 * Integer.parseInt(halfHash.get(msg.getNextnextPortProp())));
					DataOutputStream dos2 = new DataOutputStream(socket2.getOutputStream());
					dos1.writeUTF(msg.getInsertionType() + "," + msg.getKeyValue()+","+msg.getPortProp());
					dos2.writeUTF(msg.getInsertionType() + "," + msg.getKeyValue()+","+msg.getPortProp());
                    DataInputStream ois1 = new DataInputStream(new BufferedInputStream(socket1.getInputStream()));
                    DataInputStream ois2 = new DataInputStream(new BufferedInputStream(socket2.getInputStream()));
                    try {
                        String ack1 = ois1.readUTF();
                        if (ack1.equals("replicated")) {
                            socket1.close();
                        }
                    }
                    catch(Exception e){
                        socket1.close();
                    }
                    try {
                        String ack2 = ois2.readUTF();
                        if (ack2.equals("replicated")) {
                            socket2.close();
                        }
                    }
                    catch(Exception e){
                        socket2.close();
                    }



				}else if(msg.getInsertionType().equals("repStatus")){
					Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), 2 * Integer.parseInt(halfHash.get(nextPort)));
					DataOutputStream dos1 = new DataOutputStream(socket1.getOutputStream());
					Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), 2 * Integer.parseInt(halfHash.get(prevPort)));
					DataOutputStream dos2 = new DataOutputStream(socket2.getOutputStream());

					dos1.writeUTF(msg.getInsertionType() +",np");
					dos2.writeUTF(msg.getInsertionType()+",pp" );
					DataInputStream ois1 = new DataInputStream(new BufferedInputStream(socket1.getInputStream()));
					DataInputStream ois2 = new DataInputStream(new BufferedInputStream(socket2.getInputStream()));
                    try{
						String ack1 = ois1.readUTF();
						Log.e("client ack1",ack1);
						if(ack1.equals("shut")){
							socket1.close();
						}
						String amp1[]=ack1.split(",,");
						String pp1=amp1[0];
						String ppp1=amp1[1];
						String[] ppkeyvalue=pp1.split("&");
						String[] pppkeyvalue=ppp1.split("&");
						for(int i=0;i<ppkeyvalue.length;i++) {
							ContentValues mContentValues = new ContentValues();
							String keyvalueMsg[]=ppkeyvalue[i].split(",");
							mContentValues.put("key", keyvalueMsg[0]);
							mContentValues.put("value", keyvalueMsg[1]);
							mContentValues.put("type","dontreplicate");
							Uri newUri = insert(mUri, mContentValues);
						}
						Log.e("pp",ppkeyvalue.length+"");
						for(int i=0;i<pppkeyvalue.length;i++) {
							ContentValues mContentValues = new ContentValues();
							String keyvalueMsg[]=pppkeyvalue[i].split(",");
							mContentValues.put("key", keyvalueMsg[0]);
							mContentValues.put("value", keyvalueMsg[1]);
							Uri newUri = insertReplica(mUri, mContentValues,prevPort);
						}
						Log.e("ppp",pppkeyvalue.length+"");
					}
                    catch(Exception e){
						Socket socket3 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), 2 * Integer.parseInt(halfHash.get(nextnextPort)));
						DataOutputStream dos3 = new DataOutputStream(socket3.getOutputStream());
						Socket socket4= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), 2 * Integer.parseInt(halfHash.get(prevPort)));
						DataOutputStream dos4 = new DataOutputStream(socket4.getOutputStream());
						msg.setInsertionType("repStatusNextcCrash");
						dos3.writeUTF(msg.getInsertionType() +",nnp");
						dos4.writeUTF(msg.getInsertionType()+",pp" );
						DataInputStream dis3 = new DataInputStream(new BufferedInputStream(socket3.getInputStream()));
						DataInputStream dis4 = new DataInputStream(new BufferedInputStream(socket4.getInputStream()));
						String ack3 = dis3.readUTF();
						if(ack3.equals("shut")){
							socket3.close();
						}
						String[] pp2keyvalue=ack3.split("&");
						for(int i=0;i<pp2keyvalue.length;i++) {
							ContentValues mContentValues = new ContentValues();
							String keyvalueMsg[]=pp2keyvalue[i].split(",");
							mContentValues.put("key", keyvalueMsg[0]);
							mContentValues.put("value", keyvalueMsg[1]);
							mContentValues.put("type","dontreplicate");
							Uri newUri = insert(mUri, mContentValues);
						}

						String ack4=dis4.readUTF();
						if(ack4.equals("shut")){
							socket4.close();
						}
						String amp1[]=ack4.split(",,");
						String pp1=amp1[0];
						String ppp1=amp1[1];
						String[] ppkeyvalue=pp1.split("&");
						String[] pppkeyvalue=ppp1.split("&");
						for(int i=0;i<ppkeyvalue.length;i++) {
							ContentValues mContentValues = new ContentValues();
							String keyvalueMsg[]=ppkeyvalue[i].split(",");
							mContentValues.put("key", keyvalueMsg[0]);
							mContentValues.put("value", keyvalueMsg[1]);
							//mContentValues.put("type","dontreplicate");
							Uri newUri = insertReplica(mUri, mContentValues,PrevPrevPort);
						}

						for(int i=0;i<pppkeyvalue.length;i++) {
							ContentValues mContentValues = new ContentValues();
							String keyvalueMsg[]=pppkeyvalue[i].split(",");
							mContentValues.put("key", keyvalueMsg[0]);
							mContentValues.put("value", keyvalueMsg[1]);
							Uri newUri = insertReplica(mUri, mContentValues,prevPort);
						}

socket4.close();

					}

					try{
						String ack2 = ois2.readUTF();

						Log.e("client ack2",ack2);
						if(ack2.equals("shut")){
							socket2.close();
						}
						String[] pp2keyvalue=ack2.split("&");
						for(int i=0;i<pp2keyvalue.length;i++) {
							ContentValues mContentValues = new ContentValues();
							String keyvalueMsg[]=pp2keyvalue[i].split(",");
							mContentValues.put("key", keyvalueMsg[0]);
							mContentValues.put("value", keyvalueMsg[1]);
							Uri newUri = insertReplica(mUri, mContentValues,"");
						}
						Log.e("pp2",pp2keyvalue.length+"");
						//socket1.close();
						socket2.close();
					}
					catch(Exception e){
						Socket socket3 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), 2 * Integer.parseInt(halfHash.get(PrevPrevPort)));
						DataOutputStream dos3 = new DataOutputStream(socket3.getOutputStream());
						dos3.writeUTF(msg.getInsertionType() +",ppp");
						DataInputStream dis3= new DataInputStream(new BufferedInputStream(socket3.getInputStream()));
						try{
							String ack3 = dis3.readUTF();
							Log.e("client ack3",ack3);
							if(ack3.equals("shut")){
								socket3.close();
							}
							String[] pppkeyvalue=ack3.split("&");
							for(int i=0;i<pppkeyvalue.length;i++) {
								ContentValues mContentValues = new ContentValues();
								String keyvalueMsg[]=pppkeyvalue[i].split(",");
								mContentValues.put("key", keyvalueMsg[0]);
								mContentValues.put("value", keyvalueMsg[1]);
								Uri newUri = insertReplica(mUri, mContentValues,"");
							}
							//Log.e("pppp",pppkeyvalue.length+"");
							socket3.close();
						}
						catch(Exception e1){

						}

					}





				}
			} catch (Exception e) {
			    e.printStackTrace();
			}
		}

		public String[] ans() {
			try {
				if (msg.getInsertionType().equals("query")) {
					Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), 2 * Integer.parseInt(halfHash.get(msg.getPortProp())));
					DataOutputStream oos1 = new DataOutputStream(socket1.getOutputStream());
					DataInputStream ois1 = new DataInputStream(new BufferedInputStream(socket1.getInputStream()));
					oos1.writeUTF("query," + msg.getKeyValue());
					String msg = ois1.readUTF();
					if (msg.equals("searching")) {
						socket1.close();
					} else {
						socket1.close();
						String[] trial = msg.split(",");
						return trial;
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}	return null;
		}

		public ArrayList<String> allAns() {
			ArrayList<String> as = new ArrayList<String>();
			try {
				if(msg.getInsertionType().equals("all")) {
					Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), 2* Integer.parseInt(msg.getPortProp()));
					DataOutputStream oos1 = new DataOutputStream(socket1.getOutputStream());
					DataInputStream ois1 = new DataInputStream(new BufferedInputStream(socket1.getInputStream()));
					oos1.writeUTF("all");
					String mes= ois1.readUTF();
					if(!mes.equals("")) {
						String[] keyValue = mes.split(",");
						for (String s : keyValue) {
							String[] star = s.split("&");
							as.add(star[0] + "," + star[1]);
						}socket1.close();
					}
					socket1.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			return as;
		}
		public String deleteQuery(){
			try {
				if (msg.getInsertionType().equals("delete")) {
					Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), 2 * Integer.parseInt(halfHash.get(msg.getPortProp())));
					DataOutputStream oos1 = new DataOutputStream(socket1.getOutputStream());
					try {
                        oos1.writeUTF("delete," + msg.getKeyValue());
                        DataInputStream ois1 = new DataInputStream(new BufferedInputStream(socket1.getInputStream()));
                        String deleteAck = ois1.readUTF();
                        if (deleteAck.equals("deleted")) {
                            socket1.close();
                        }
                    }
                    catch (Exception e){
					    e.printStackTrace();
					    socket1.close();
                    }

				}
			}
			catch(Exception e){
			    e.printStackTrace();
			}
			return null;
		}
	}
	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}
	public class Server extends Thread {
		private Uri AUri = null;
		Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
		public void run() {
            Socket client= new Socket();
            try {

				ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
				while (true) {
					client = serverSocket.accept();
					DataInputStream ois = new DataInputStream(new BufferedInputStream(client.getInputStream()));
					String m = ois.readUTF();
					Log.e("Server ",m);
					if(m.contains("normal")) {
					//	Log.e("server",portStr);
						String[] keyvalueMsg = m.split(",");
						ContentValues mContentValues = new ContentValues();
						mContentValues.put("key", keyvalueMsg[1]);
						mContentValues.put("value", keyvalueMsg[2]);
						mContentValues.put("type","dontreplicate");
						Uri newUri = getContext().getContentResolver().insert(mUri, mContentValues);
						DataOutputStream dos = new DataOutputStream(client.getOutputStream());
						dos.writeUTF("done");
					}else if(m.contains("replication")){
							String[] keyvalueMsg = m.split(",");
							ContentValues mContentValues = new ContentValues();
							mContentValues.put("key", keyvalueMsg[1]);
							mContentValues.put("value", keyvalueMsg[2]);
							insertReplica(mUri, mContentValues,keyvalueMsg[3]);
							DataOutputStream dos = new DataOutputStream(client.getOutputStream());
							dos.writeUTF("replicated");
						}
						else if (m.contains("query")){
						String[] mg = m.split(",");
						String[] proj = {"key", "value"};
						Cursor q = query(mUri, proj, mg[1], null, "");
						DataOutputStream dos = new DataOutputStream(client.getOutputStream());
						if (q == null) {
							dos.writeUTF("searching");
						} else {
							if (q.getCount() >= 1) {
								while (q.moveToNext()) {
									String key = q.getString(0);
									String value = q.getString(1);
									dos.writeUTF(key + "," + value);
								}
							}

						}
					}else if (m.equals("all")){
						DataOutputStream dos = new DataOutputStream(client.getOutputStream());
						String[] proj = {"key", "value"};
						Cursor q = query(mUri, proj, "@", null, "");
						String foundall="";
						if (q.getCount() >= 1) {

							while (q.moveToNext()) {
								String key = q.getString(0);
								String value = q.getString(1);
								String keyValue=key+"&"+value;
								foundall=foundall+keyValue+",";
							}
						}dos.writeUTF(foundall);
					}else if(m.contains("delete")){
						//String[] proj = {"key", "value"};
						String deleteMsgs[]=m.split(",");
						 int a = delete(mUri, deleteMsgs[1], new String[]{"dontdelete"});
                        DataOutputStream dos = new DataOutputStream(client.getOutputStream());
                        dos.writeUTF("deleted");

					}else if(m.contains("repStatus")){
							if(replicationStatus!=null ) {
								String mess[] = m.split(",");
								String allRepl = "";
								String ppall = "";
								String pppall = "";
								if (mess[1].equals("np")) {
									for (Map.Entry<String, String> hm : replicationStatus.entrySet()) {

                                        String splitValue[]=hm.getValue().split(",");

                                        if(splitValue[0].equals("pp")){
                                              ppall = ppall + hm.getKey()+","+splitValue[1] + "&";
                                         }
                                         else if (splitValue[0].equals("ppp")) {
										 	pppall = pppall + hm.getKey()+","+splitValue[1] + "&";
										}
									}
									DataOutputStream dos = new DataOutputStream(client.getOutputStream());
									dos.writeUTF(ppall + ",," + pppall);
								} else if (mess[1].equals("pp")) {
									for (Map.Entry<String, String> hm : replicationStatus.entrySet()) {
                                        String splitValue[]=hm.getValue().split(",");
											if (splitValue[0].equals("pp")) {
												allRepl = allRepl + hm.getKey()+","+splitValue[1] + "&";
											}
									}

									DataOutputStream dos = new DataOutputStream(client.getOutputStream());
									dos.writeUTF(allRepl);


								}else if (mess[1].equals("ppp")) {
								if(myFiles!=null) {
									for (String s : myFiles) {

										String[] proj = {"key", "value"};
										Cursor q = query(mUri, proj, s, null, "");

										if (q == null) {
											DataOutputStream dos = new DataOutputStream(client.getOutputStream());
											dos.writeUTF(allRepl);
										} else {
											if (q.getCount() >= 1) {
												while (q.moveToNext()) {
													String key = q.getString(0);
													String value = q.getString(1);
													allRepl = allRepl + key + "," + value + "&";
												}
											}

										}
									}
								}
									DataOutputStream dos = new DataOutputStream(client.getOutputStream());
									dos.writeUTF(allRepl);
								}
							}else{
							    Log.e("server","shut");
								DataOutputStream dos = new DataOutputStream(client.getOutputStream());
								dos.writeUTF("shut");
							}
					}else if(m.contains("repStatusNextcCrash")){
						if(replicationStatus!=null ) {
							String mess[] = m.split(",");
							String allRepl = "";
							String ppall = "";
							String pppall = "";

							 if (mess[1].equals("pp")) {
								for (Map.Entry<String, String> hm : replicationStatus.entrySet()) {
									String splitValue[]=hm.getValue().split(",");
									if (splitValue[0].equals("pp")) {
										ppall=ppall + hm.getKey()+","+splitValue[1] + "&";
									}
								}
								if(myFiles!=null) {
									for (String s : myFiles) {
										String[] proj = {"key", "value"};
										Cursor q = query(mUri, proj, s, null, "");

										if (q != null) {

											if (q.getCount() >= 1) {
												while (q.moveToNext()) {
													String key = q.getString(0);
													String value = q.getString(1);
													pppall = pppall + key + "," + value + "&";
												}
											}

										}
									}
								}

									DataOutputStream dos = new DataOutputStream(client.getOutputStream());
								allRepl=ppall+",,"+pppall;
									dos.writeUTF(allRepl);


							}else if (mess[1].equals("nnp")) {

									 for (Map.Entry<String, String> hm : replicationStatus.entrySet()) {
										 String splitValue[]=hm.getValue().split(",");
										 if (splitValue[0].equals("ppp")) {
											 allRepl = allRepl + hm.getKey()+","+splitValue[1] + "&";
										 }
									 }

									 DataOutputStream dos = new DataOutputStream(client.getOutputStream());
									 dos.writeUTF(allRepl);
							}
						}else{
							Log.e("server","shut");
							DataOutputStream dos = new DataOutputStream(client.getOutputStream());
							dos.writeUTF("shut");
						}
					}

				}
			} catch (Exception e) {
            e.printStackTrace();

			}
			finally {
			    try{
                    client.close();
                }
                catch (Exception e){
                e.printStackTrace();
                }
            }
		}
	}
	public class Message{
		String portProp;
		String keyValue;
		String insertionType;
		String nextPortProp;
		String nextnextPortProp;

		public String getNextPortProp() {
			return nextPortProp;
		}

		public void setNextPortProp(String nextPortProp) {
			this.nextPortProp = nextPortProp;
		}

		public String getNextnextPortProp() {
			return nextnextPortProp;
		}

		public void setNextnextPortProp(String nextnextPortProp) {
			this.nextnextPortProp = nextnextPortProp;
		}

		public String getInsertionType() {
			return insertionType;
		}

		public void setInsertionType(String insertionType) {
			this.insertionType = insertionType;
		}

		public String getPortProp() {
			return portProp;
		}

		public void setPortProp(String portProp) {
			this.portProp = portProp;
		}

		public String getKeyValue() {
			return keyValue;
		}

		public void setKeyValue(String keyValue) {
			this.keyValue = keyValue;
		}
	}
	public Uri insertReplica(Uri uri, ContentValues values,String s) {
			// TODO Auto-generated method stub

			String msg_key = values.getAsString("key");
			String msg_value = values.getAsString("value");
			Context con = getContext();
			try {
				FileOutputStream out = con.openFileOutput(msg_key, Context.MODE_PRIVATE);
				out.write(msg_value.getBytes());
				out.close();
				Log.e("updation",msg_key+" with "+msg_value);
				if(s.equals(prevPort)) {
					replicationStatus.put(msg_key, "pp"+","+msg_value);
				}else {
					replicationStatus.put(msg_key, "ppp"+","+msg_value);
				}
				myFiles.add(msg_key);
			}
			catch(Exception e){
                e.printStackTrace();
			}
		return uri;
	}




}
