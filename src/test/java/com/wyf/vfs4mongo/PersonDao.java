package com.wyf.vfs4mongo;

import org.springframework.data.mongodb.repository.MongoRepository;

public interface PersonDao extends MongoRepository<Person,String> {



}
