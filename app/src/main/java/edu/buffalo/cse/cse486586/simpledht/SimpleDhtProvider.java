package edu.buffalo.cse.cse486586.simpledht;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {
    private static DBHelper dbh;
    private final Uri mUri;
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";

    private final int SERVER_PORT = 10000;
    private final String[] REMOTE_PORT = {"11108","11112","11116","11120","11124"};
    private static ArrayList<String> liveNodes;
    private static HashMap<String,String> hm; //stores the hashed nodeIDs with the corresponding port numbers
    private static String successor = "";
    private static String predecessor = "";
    private static String myNodeId = "";
    private static String myEmulator = "";
    private static SimpleDhtProvider sdp = new SimpleDhtProvider();
    private static MessagePack queryMsg = null;
    private static int countResponse = 0;
    private boolean queryMsgFlag = false;
    private ArrayList<MessagePack> starResponse;

    public SimpleDhtProvider(){
        super();
        liveNodes = new ArrayList<String>();
        hm = new HashMap<String, String>();
        mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
        starResponse = new ArrayList<MessagePack>();
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        Cursor cursor = dbh.getWritableDatabase().query(dbh.getDatabaseName(),null,"key=\"" + selection + "\"",selectionArgs,null,null,null);
        if(cursor.getCount()>0){
            Log.e("Deleted", selection);
            return dbh.getWritableDatabase().delete(dbh.getDatabaseName(),"key=\"" + selection + "\"",selectionArgs);
        }
        else{
            MessagePack delMsg = new MessagePack(myNodeId,selection,"","DELETE");
            new ClientTaskQuery().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, delMsg);
            return 0;
        }
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        String keyI = (String) values.get(KEY_FIELD);
        try {
            String keyHash = this.genHash(keyI);
            if(predecessor=="" && successor=="") { //if only one avd is alive then insert directly
                dbh.getWritableDatabase().insert(dbh.getDatabaseName(),null,values);
                Log.v("insert1", values.toString());
            }
            else if(keyHash.compareTo(predecessor)>0 && myNodeId.compareTo(keyHash)>0) { //if the current avd is the correct position then insert otherwise create a client task
                dbh.getWritableDatabase().insert(dbh.getDatabaseName(),null,values);
                Log.v("insert2", values.toString());
            }
            else if(keyHash.compareTo(predecessor)>0 && keyHash.compareTo(myNodeId)>0 && myNodeId.equals(liveNodes.get(0))) {
                dbh.getWritableDatabase().insert(dbh.getDatabaseName(),null,values);
                Log.v("insert3", values.toString());
            }
            else if(keyHash.compareTo(myNodeId)<0 && myNodeId.equals(liveNodes.get(0))) {
                dbh.getWritableDatabase().insert(dbh.getDatabaseName(),null,values);
                Log.v("insert4", values.toString());
            }
            else {
                new ClientTaskInsert().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, values);
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public boolean onCreate() {
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        myEmulator = tel.getLine1Number().substring(7);
        try {
            myNodeId = this.genHash(myEmulator);
            Log.e(myEmulator,myNodeId);
            hm.put(this.genHash("5554"),"11108"); //storing all the hashed nodeIds and the corresponding port numbers in the hashmap
            hm.put(this.genHash("5556"),"11112");
            hm.put(this.genHash("5558"),"11116");
            hm.put(this.genHash("5560"),"11120");
            hm.put(this.genHash("5562"), "11124");

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        dbh = new DBHelper(getContext());

        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myNodeId, myEmulator);

        try{
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e){
            Log.e("ServerSocketError", "Can't create a ServerSocket");
        }

        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
        Log.v("query", selection);

        if(selection.equals("*") || selection.equals("\"*\"")){
            Cursor cursor = dbh.getWritableDatabase().rawQuery("select * from KMDB", null);

            if (predecessor=="" && successor=="") { //if only one avd is alive then fetch its contents and return
                return cursor;
            } else {                               //else send a "@" mseeage to all the avds
                queryMsg = new MessagePack(myNodeId,"@","","QUERY");
                new ClientTaskQuery().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryMsg);
                while(countResponse<liveNodes.size()-1){} //wait until response from all the avds is received
                countResponse = 0;

                MatrixCursor cur = new MatrixCursor(new String[]{"key", "value"});

                for(MessagePack response : starResponse){  //club the response received from the avds into one cursor object
                    String keyR = response.key;
                    keyR = keyR.substring(1,keyR.length()-1);
                    String valR = response.value;
                    valR = valR.substring(1,valR.length()-1);
                    String keyAr[] = keyR.split(", ");
                    String valAr[] = valR.split(", ");
                    for(int i=0;i<keyAr.length;i++){
                        cur.addRow(new Object[]{keyAr[i],valAr[i]});
                    }
                }
                cursor.moveToFirst(); //add own data to the returning cursor as well
                for(int i=0;i<cursor.getCount();i++){
                    String keyField = cursor.getString(0);
                    String valField = cursor.getString(1);
                    cur.addRow(new Object[]{keyField,valField});
                    cursor.moveToNext();
                }

                queryMsg = null;
                starResponse = null;
                return cur;
            }
        }
        else if(selection.equals("@") || selection.equals("\"@\"")){
            Cursor cursor = dbh.getWritableDatabase().rawQuery("select * from KMDB", null);
            if ((predecessor=="" && successor=="")||queryMsg==null) {
                return cursor;
            }
            else if(!queryMsg.nodeId.equals(myNodeId)){
                new ClientTaskQuery().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryMsg); //forward the request to the successor as well
                queryMsgFlag = true;

                cursor.moveToFirst();
                int len = cursor.getCount();
                ArrayList<String> keyA = new ArrayList<String>();
                ArrayList<String> valA = new ArrayList<String>();
                for(int i=0;i<len;i++){
                    keyA.add(i,cursor.getString(0));
                    valA.add(i, cursor.getString(1));
                    cursor.moveToNext();
                }

                while(queryMsgFlag==true); //prevent concurrent read and write

                queryMsg.msgType = "RESPONSE@";
                queryMsg.key = keyA.toString();
                queryMsg.value = valA.toString();
                new ClientTaskQuery().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryMsg); //send the response to original sender
            }
            return null;
        }
        else{
            Cursor cursor = dbh.getWritableDatabase().query(dbh.getDatabaseName(),projection,"key=\"" + selection + "\"",selectionArgs,sortOrder,null,null);

            if(cursor.getCount()==0){
                if(queryMsg == null)
                    queryMsg = new MessagePack(myNodeId,selection,"","QUERY");
                new ClientTaskQuery().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryMsg);

                if(queryMsg!=null && !queryMsg.nodeId.equals(myNodeId)){
                    return null;
                }

                while (!queryMsg.msgType.equals("RESPONSE")) {
                }

                MatrixCursor cur = new MatrixCursor(new String[]{"key", "value"}, 1);
                cur.addRow(new Object[]{queryMsg.key, queryMsg.value});
                queryMsg = null;
                return cur;
            }
            else{
                if(queryMsg==null || queryMsg.nodeId.equals(myNodeId)) {
                    queryMsg = null;
                    return cursor;
                }
                else{
                    cursor.moveToFirst();
                    if(queryMsg.key.equals(cursor.getString(0))){
                        queryMsg.value = cursor.getString(1);
                        queryMsg.msgType = "RESPONSE";
                        new ClientTaskQuery().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryMsg);
                    }
                    return null;
                }
            }
        }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    public String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {
            boolean flag = false;

            if (!myEmulator.equals("5554") && liveNodes.size() == 0) { //when an avd gets alive it will inform avd 5554
                try {
                    MessagePack nodeIdMsg = new MessagePack(myNodeId,"","","AVD_LIVE");
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt("11108"));
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    out.writeObject(nodeIdMsg);
                    out.close();
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return null;
            }

            if (myEmulator.equals("5554") && liveNodes.size() > 0) {//after all the avds are alive 5554 informs all the avds about all the live avds
                Collections.sort(liveNodes);
                String avds = liveNodes.toString().substring(1,liveNodes.toString().length()-1);
                MessagePack liveAvds = new MessagePack(avds,"","","LIVE_AVD_ARRAY");
                String myPort = Integer.parseInt(myEmulator) * 2 + "";
                for(String liveN : liveNodes) {
                    String remotePort = hm.get(liveN);
                    if(!remotePort.equals(myPort)) {
                        try {
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
                            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                            out.writeObject(liveAvds);
                            out.close();
                            socket.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }

                int i = liveNodes.indexOf(myNodeId);  //sets successor and predecessor values
                successor = liveNodes.get((i + 1) % liveNodes.size());
                if(i==0)
                    predecessor = liveNodes.get(liveNodes.size()-1);
                else
                    predecessor = liveNodes.get((i-1)%liveNodes.size());
            }

            return null;
        }
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {

            ServerSocket serverSocket = sockets[0];
            while(true) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream());

                    MessagePack incomingMsg = (MessagePack) in.readObject();

                    if(myEmulator.equals("5554")&&incomingMsg.msgType.equals("AVD_LIVE")) {//avd 5554 tracks all the new avd join requests
                        if (!liveNodes.contains(myNodeId))
                            liveNodes.add(myNodeId);
                        liveNodes.add(incomingMsg.nodeId);
                        this.publishProgress(""); //as soon as a new avd joins avd 5554 will inform everyone abt the live avds by calling ClientTask
                    }
                    else if(!myEmulator.equals("5554")&&incomingMsg.msgType.equals("LIVE_AVD_ARRAY")) { //receives information about all the live avds
                        String[] nodeArray = incomingMsg.nodeId.split(", ");
                        for(String s:nodeArray){
                            if(!liveNodes.contains(s))
                                liveNodes.add(s);
                        }
                        Collections.sort(liveNodes);
                        int i = liveNodes.indexOf(myNodeId); //sets successor and predecessor values
                        successor = liveNodes.get((i + 1) % liveNodes.size());
                        if(i==0)
                            predecessor = liveNodes.get(liveNodes.size()-1);
                        else
                            predecessor = liveNodes.get((i-1) % liveNodes.size());
                    }
                    else if(incomingMsg.msgType.equals("INSERT")){
                        ContentValues cv = new ContentValues();
                        cv.put(KEY_FIELD,incomingMsg.key);
                        cv.put(VALUE_FIELD,incomingMsg.value);
                        sdp.insert(mUri, cv);
                    }
                    else if(incomingMsg.msgType.equals("QUERY")){
                        queryMsg = incomingMsg;
                        Cursor resultCursor = sdp.query(mUri, null, incomingMsg.key, null, null);
                    }
                    else if(incomingMsg.msgType.equals("RESPONSE")){
                        queryMsg = incomingMsg;
                    }
                    else if(incomingMsg.msgType.equals("RESPONSE@")){
                        starResponse.add(incomingMsg); //add response from all the avds to a global array.
                        queryMsg = incomingMsg;
                        countResponse++;
                    }
                    else if(incomingMsg.msgType.equals("DELETE")){
                        sdp.delete(mUri,incomingMsg.key,null);
                    }
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }
        protected void onProgressUpdate(String...strings) {
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "hi");
        }
    }

    private class ClientTaskInsert extends AsyncTask<ContentValues, Void, Void> {
        @Override
        protected Void doInBackground(ContentValues... values) {
            String remotePort = hm.get(successor);
            MessagePack forwardedPack = new MessagePack(myNodeId,(String)values[0].get(KEY_FIELD),(String)values[0].get(VALUE_FIELD),"INSERT");
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                out.writeObject(forwardedPack);
                out.close();
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
    }

    private class ClientTaskQuery extends AsyncTask<MessagePack, Void, Void> {
        @Override
        protected Void doInBackground(MessagePack... packs) {
            String remotePort = "";
            if(packs[0].msgType.equals("QUERY")) {
                remotePort = hm.get(successor);
                if(queryMsg!=null && !queryMsg.nodeId.equals(myNodeId) && !queryMsg.key.equals("@")){
                    queryMsg = null;
                }
            }
            else if(packs[0].msgType.equals("RESPONSE") || packs[0].msgType.equals("RESPONSE@")) {
                remotePort = hm.get(queryMsg.nodeId);
                if(queryMsg!=null && !queryMsg.nodeId.equals(myNodeId)){
                    queryMsg = null;
                }
            }
            else if(packs[0].msgType.equals("DELETE")) {
                remotePort = hm.get(successor);
            }

            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                out.writeObject(packs[0]);
                if(!packs[0].msgType.equals("DELETE")) {
                    queryMsgFlag = false; //flag set to prevent lock the write access to queryMsg object
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            return null;
        }
    }
}