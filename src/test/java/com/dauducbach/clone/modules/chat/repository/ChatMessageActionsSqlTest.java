package com.dauducbach.clone.modules.chat.repository;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.nio.file.*;
import java.util.concurrent.*;
import static org.assertj.core.api.Assertions.*;

class ChatMessageActionsSqlTest {
    Connection db;
    String url="jdbc:h2:mem:messageActions;MODE=MySQL;DB_CLOSE_DELAY=-1;LOCK_TIMEOUT=5000";
    @BeforeEach void setup() throws Exception {
        db=DriverManager.getConnection(url);db.createStatement().execute("DROP ALL OBJECTS");
        db.createStatement().execute("CREATE TABLE conversations(id VARCHAR(36) PRIMARY KEY,last_message_seq BIGINT,last_message_at TIMESTAMP)");
        db.createStatement().execute("CREATE TABLE messages(id VARCHAR(36) PRIMARY KEY,conversation_id VARCHAR(36),sender_id VARCHAR(64),client_message_id VARCHAR(36),message_seq BIGINT,message_type VARCHAR(16),content VARCHAR(255),metadata VARCHAR(255),reply_to_seq BIGINT,deleted_at TIMESTAMP,reaction_version BIGINT DEFAULT 0,UNIQUE(sender_id,client_message_id))");
        db.createStatement().execute("CREATE TABLE media(asset_id VARCHAR(255) PRIMARY KEY,owner_id VARCHAR(64),owner_type VARCHAR(32))");
        db.createStatement().execute("CREATE TABLE message_reactions(message_id VARCHAR(36),user_id VARCHAR(64),reaction VARCHAR(16))");
        db.createStatement().execute("CREATE TABLE chat_reaction_outbox(id VARCHAR(36),payload VARCHAR(255))");
        String migration=Files.readString(Path.of("src/main/resources/db/manual/chat_message_actions_schema.sql")).replaceAll("(?m)^--.*$","").replace(" ENGINE=InnoDB","");
        for(String sql:migration.split(";"))if(!sql.isBlank())db.createStatement().execute(sql);
        db.createStatement().execute("INSERT INTO conversations(id,last_message_seq,last_message_at) VALUES('c',5,TIMESTAMP '2026-10-06 10:00:00')");
        db.createStatement().execute("INSERT INTO messages(id,conversation_id,sender_id,client_message_id,message_seq,message_type,content,metadata,reply_to_seq) VALUES('source','c','owner','key1',5,'IMAGE','secret','media',1),('copy','c','me','key2',6,'IMAGE','secret','media',NULL),('copy2','c','me','key3',7,'IMAGE','secret','media',NULL)");
        db.createStatement().execute("INSERT INTO media VALUES('asset','source','CHAT_MESSAGE')");
    }
    @AfterEach void close()throws Exception{db.close();}
    String refs(String id,String source){return ChatMessageActionsRepository.REFERENCE_SQL.replace(":id","'"+id+"'").replace(":source","'"+source+"'");}
    String recall(){return ChatMessageActionsRepository.RECALL_SQL.replace(":id","'source'").replace(":at","CURRENT_TIMESTAMP");}
    @Test void forwardReferencesPreserveOwnerAndSurviveOriginalRecall()throws Exception{
        db.createStatement().execute(refs("copy","source"));db.createStatement().execute(refs("copy2","copy"));
        db.createStatement().execute(recall());
        try(var row=db.createStatement().executeQuery("SELECT owner_id FROM media WHERE asset_id='asset'")){assertThat(row.next()).isTrue();assertThat(row.getString(1)).isEqualTo("source");}
        try(var row=db.createStatement().executeQuery("SELECT COUNT(*) FROM chat_message_media_refs")){row.next();assertThat(row.getInt(1)).isEqualTo(2);}
        assertThatThrownBy(()->db.createStatement().execute("DELETE FROM media WHERE asset_id='asset'")).isInstanceOf(SQLException.class);
    }
    @Test void recallSqlIsPermanentAndKeepsSequenceAndThreadTime()throws Exception{
        assertThat(db.createStatement().executeUpdate(recall())).isEqualTo(1);assertThat(db.createStatement().executeUpdate(recall())).isZero();
        try(var row=db.createStatement().executeQuery("SELECT content,metadata,reply_to_seq,message_seq,reaction_version FROM messages WHERE id='source'")){row.next();assertThat(row.getString(1)).isNull();assertThat(row.getString(2)).isNull();assertThat(row.getObject(3)).isNull();assertThat(row.getLong(4)).isEqualTo(5);assertThat(row.getLong(5)).isEqualTo(1);}
        try(var row=db.createStatement().executeQuery("SELECT last_message_seq,last_message_at FROM conversations WHERE id='c'")){row.next();assertThat(row.getLong(1)).isEqualTo(5);assertThat(row.getTimestamp(2)).isEqualTo(Timestamp.valueOf("2026-10-06 10:00:00"));}
    }
    @Test void transactionRollbackRestoresRecallPinVersionAndOutbox()throws Exception{
        db.createStatement().execute("INSERT INTO chat_message_pins VALUES('c','source','me',CURRENT_TIMESTAMP)");db.setAutoCommit(false);
        db.createStatement().execute(recall());db.createStatement().execute("DELETE FROM chat_message_pins WHERE message_id='source'");db.createStatement().execute("UPDATE conversations SET pin_version=pin_version+1 WHERE id='c'");db.createStatement().execute("INSERT INTO chat_reaction_outbox VALUES('event','{}')");db.rollback();
        try(var row=db.createStatement().executeQuery("SELECT deleted_at FROM messages WHERE id='source'")){row.next();assertThat(row.getObject(1)).isNull();}
        try(var row=db.createStatement().executeQuery("SELECT pin_version FROM conversations WHERE id='c'")){row.next();assertThat(row.getLong(1)).isZero();}
        try(var row=db.createStatement().executeQuery("SELECT COUNT(*) FROM chat_message_pins")){row.next();assertThat(row.getInt(1)).isEqualTo(1);}
        try(var row=db.createStatement().executeQuery("SELECT COUNT(*) FROM chat_reaction_outbox")){row.next();assertThat(row.getInt(1)).isZero();}
    }
    @Test void conversationLockSerializesCompetingPinAndRecall()throws Exception{
        db.setAutoCommit(false);db.createStatement().executeQuery("SELECT id FROM conversations WHERE id='c' FOR UPDATE").close();
        var executor=Executors.newSingleThreadExecutor();var started=new CountDownLatch(1);
        try {var second=executor.submit(()->{try(var conn=DriverManager.getConnection(url)){conn.setAutoCommit(false);started.countDown();conn.createStatement().executeQuery("SELECT id FROM conversations WHERE id='c' FOR UPDATE").close();try(var row=conn.createStatement().executeQuery("SELECT deleted_at FROM messages WHERE id='source'")){row.next();boolean deleted=row.getObject(1)!=null;
            if(!deleted)conn.createStatement().execute("INSERT INTO chat_message_pins VALUES('c','source','peer',CURRENT_TIMESTAMP)");
            conn.commit();return deleted;}}});
            assertThat(started.await(2,TimeUnit.SECONDS)).isTrue();assertThatThrownBy(()->second.get(100,TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
            db.createStatement().execute(recall());db.commit();assertThat(second.get(2,TimeUnit.SECONDS)).isTrue();
            try(var row=db.createStatement().executeQuery("SELECT COUNT(*) FROM chat_message_pins")){row.next();assertThat(row.getInt(1)).isZero();}
        }finally{db.rollback();executor.shutdownNow();}
    }
}
