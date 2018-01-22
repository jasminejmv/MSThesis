import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.ObjectOutputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.charset.MalformedInputException;
import java.nio.file.StandardOpenOption;
import java.io.IOException;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.Database;
import com.sleepycat.db.DatabaseConfig;
import com.sleepycat.db.DatabaseType;
import com.sleepycat.db.Transaction;
import com.sleepycat.db.OperationStatus;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.DeadlockException;
import com.sleepycat.db.HashStats;
import com.sleepycat.db.StatsConfig;
import com.sleepycat.db.DatabaseStats;
import com.sleepycat.db.Cursor;
import com.sleepycat.db.LockMode;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;


public class graphIMDbComplete21
{
	
	Database bdbMovieId,bdbYearId, bdbEpisodeId, bdbTvId;
	Database bdbProducerMovie, bdbProducerTv, bdbProducerEpisode, bdbMovieProducer;
	Database bdbEpisodeProducer, bdbTvProducer;
	Database bdbActorId, bdbActressId;
	Database bdbCountryId, bdbDirectorId, bdbProducerId, bdbGenreId, bdbLanguageId;
	Database bdbMovieYear,bdbTvYear, bdbEpisodeYear, bdbYearEpisode,bdbYearMovie,bdbYearTv;
	Database bdbActorMovie, bdbActorEpisode, bdbActorTv,bdbMovieActor, bdbEpisodeActor, bdbTvActor ;
	Database bdbActressMovie,bdbMovieActress, bdbTvActress, bdbActressEpisode, bdbActressTv, bdbEpisodeActress;
	Database bdbCountryMovie, bdbCountryTv, bdbCountryEpisode, bdbMovieCountry, bdbTvCountry, bdbEpisodeCountry;
	Database bdbDirectorMovie, bdbDirectorEpisode, bdbDirectorTv, bdbMovieDirector, bdbEpisodeDirector, bdbTvDirector ;
	Database bdbGenreEpisode, bdbGenreMovie, bdbGenreTv, bdbEpisodeGenre, bdbMovieGenre, bdbTvGenre;
	Database bdbLanguageEpisode,bdbLanguageTv, bdbLanguageMovie, bdbMovieLanguage, bdbEpisodeLanguage, bdbTvLanguage;
	Database bdbPerson, bdbPersonId, bdbEpisodeTv,bdbTvEpisode;
	Database bdbLocCountryId, bdbLocCityId, bdbLocStateId;
	Database bdbEpisodeLoc, bdbLocCountryEpisode, bdbLocStateEpisode, bdbLocCityEpisode;
	Database  bdbTvLoc, bdbLocCountryTv, bdbLocStateTv, bdbLocCityTv;
	Database bdbMovieLoc, bdbLocCountryMovie, bdbLocStateMovie, bdbLocCityMovie;
	DatabaseConfig dbConfig;
	
	BufferedReader reader;
	BufferedWriter writer ; // =Files.newBufferedReader(pathSourceFile,charset);;
	
	Cursor cursorPerson;
		
	Charset charset;

	long  totalRows,movieRows,movieErrorRows,episodeRows,episodeErrorRows, episodeYearErrorRows;
	long longMovieId,longYearId,longEpisodeId,longTvId,longActorId,longActressId,longCountryId; 
	long longDirectorId,longProducerId, longGenreId;
	long longLanguageId,longLocationId;
	long longPersonId;
	long longLocCityId, longLocStateId, longLocCountryId;
	DatabaseEntry dbPerson, dbPersonValue; 
	String stringPersonValue;
	byte[] bytePersonValue;
	String stringPersonFile;
	
	long repeatMovieRows;
	int matchPositionYear,matchPositionEpisode;
	String line;
	String stringMovie,stringMovieId; //stringYearDigits;
	String stringEpisodeId, stringEpisode;//stringEpisodeYear;
	String stringYearId,stringYear;
	String stringTvId,stringTv;

	String stringMovieFile, stringTvFile, stringEpisodeFile, stringYearFile, stringFile;
	String stringCityFile, stringStateFile, stringCountryFile;
	HashMap<String,String> mapFiles= new HashMap<String,String>();
	String stringEdge;
	//OpenOption[] options=new OpenOption[] {WRITE,CREATE_NEW};
	
	public graphIMDbComplete21()
	{	charset = Charset.forName("ISO-8859-1"); //UTF-16, UTF-8, US-ASCII
		dbConfig = new DatabaseConfig();
		dbConfig.setAllowCreate(true);
	//	dbConfig.setSortedDuplicates(true);
		dbConfig.setUnsortedDuplicates(true);
		dbConfig.setType(DatabaseType.HASH);
		try
		{
			
			bdbMovieId=new Database("graphsHash/movieId.db",null,dbConfig);
			bdbEpisodeId=new Database("graphsHash/episodeId.db",null,dbConfig);
			bdbTvId=new Database("graphsHash/tvId.db",null,dbConfig);
			bdbYearId=new Database("graphsHash/yearId.db",null,dbConfig);

		}
		catch(Exception e)
		{	System.err.println(e);
			e.printStackTrace();
		}	
		mapFiles= new HashMap<String,String>();
		mapFiles.put("movie","m");
		mapFiles.put("year","y");
		mapFiles.put("episode","e");
		mapFiles.put("tv","t");
		mapFiles.put("actor","r");
		mapFiles.put("actress","s");
		mapFiles.put("country","c");
		mapFiles.put("director","d");
		mapFiles.put("producer","p");
		mapFiles.put("genre","g");
		mapFiles.put("language","l");
		mapFiles.put("person","n");
		mapFiles.put("locCity","a");
		mapFiles.put("locState","b");
		
		stringMovieFile=mapFiles.get("movie");
		stringEpisodeFile=mapFiles.get("episode");
		stringTvFile=mapFiles.get("tv");
		stringPersonFile=mapFiles.get("person");
		stringYearFile=mapFiles.get("year");
		
		totalRows=movieRows=movieErrorRows=episodeRows=episodeErrorRows=episodeYearErrorRows=0;
		longMovieId=longYearId=longEpisodeId=longTvId=longActorId=longActressId=longCountryId=longDirectorId=0;
		longProducerId=longGenreId=longLocationId=longLanguageId=0;
		repeatMovieRows=0;
		longPersonId=0;
		matchPositionYear=matchPositionEpisode=0;
		line = null;
	}
	

	public void updatebdbYearMovie(String stringYear,String stringYearId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue,dbValueNew;
		byte[] byteKey, byteValueNew,byteValue,byteValueOld;
		byteKey=stringYear.getBytes(charset);
		byteValue=(stringYearId+stringToAdd).getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		dbValue=new DatabaseEntry(byteValue);
		try
		{	bdbYearMovie.put(null,dbKey,dbValue);
			 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	}

	public void updatebdbYearTv(String stringYear,String stringYearId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue,dbValueNew;
		byte[] byteKey, byteValueNew,byteValue,byteValueOld;
		byteKey=stringYear.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		dbValue=new DatabaseEntry();
		try
		{	byteValueNew=(stringYearId+stringToAdd).getBytes(charset);
			dbValue=new DatabaseEntry(byteValueNew);
			bdbYearTv.put(null,dbKey,dbValue); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	}

	public void updatebdbYearEpisode(String stringYear,String stringYearId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue,dbValueNew;
		byte[] byteKey, byteValueNew,byteValue,byteValueOld;
		byteKey=stringYear.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		dbValue=new DatabaseEntry();
		try
		{	byteValueNew=(stringYearId+stringToAdd).getBytes(charset);
			dbValueNew=new DatabaseEntry(byteValueNew);
			bdbYearEpisode.put(null,dbKey,dbValueNew); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	}

	
	public String checkYear(String stringYear, Boolean booleanAdd)//, String stringToAdd)
	{	byte[] byteYear,byteYearId;
		DatabaseEntry dbYear,dbYearId;
		//String stringYearIMDb="";
		byteYear=stringYear.getBytes(charset);
		dbYear=new DatabaseEntry(byteYear);	
		dbYearId=new DatabaseEntry();//byteYearId);
		String stringYearId="";
		String [] stringValue;
		try
		{	if(bdbYearId.exists(null,dbYear)==OperationStatus.SUCCESS)
			{	if(bdbYearId.get(null,dbYear,dbYearId,null)==OperationStatus.SUCCESS)
				{	stringYearId=new String(dbYearId.getData(),charset);  //Value may contain more information besides the YearId
				}
			}
			else if(booleanAdd)
			{	longYearId++;
				stringYearId=stringYearFile +Long.toString(longYearId);
				byteYearId=stringYearId.getBytes(charset);
				dbYearId=new DatabaseEntry(byteYearId);
			//	System.out.println("Year"+stringYear+" "+stringYearIMDb + " stringtoAdd:"+stringToAdd);
				bdbYearId.put(null,dbYear,dbYearId);
			}
			
			//stringYearIMDb +="\t"+ stringToAdd;
			
			
		}
		catch(DatabaseException dbe)
		{	System.err.println("line 189"+dbe);
			dbe.printStackTrace();
		}
		
		
		return stringYearId;
	}
	
	public void updatebdbMovieYear(String stringMovie, String stringMovieId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue,dbValueNew;
		byte[] byteKey, byteValueNew,byteValue,byteValueOld;
		byteKey=stringMovie.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		dbValue=new DatabaseEntry();
		try
		{	byteValueNew=(stringMovieId+stringToAdd).getBytes(charset);
			dbValueNew=new DatabaseEntry(byteValueNew);
			bdbMovieYear.put(null,dbKey,dbValueNew); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	}

	public String checkMovie(String stringMovie,Boolean booleanAdd) //String stringToAdd)
	{	byte[] byteMovie,byteMovieId;
		DatabaseEntry dbMovie,dbMovieId;
		//String stringMovieIMDb="";
		byteMovie=stringMovie.getBytes(charset);
		dbMovie=new DatabaseEntry(byteMovie);
		dbMovieId=new DatabaseEntry();
		//stringMovieId="";
		String stringMovieId="";
		String [] stringValue;
		try
		{	if (bdbMovieId.exists(null,dbMovie)==OperationStatus.SUCCESS)
			{	if(bdbMovieId.get(null,dbMovie,dbMovieId,null)==OperationStatus.SUCCESS)
				{	stringMovieId=new String(dbMovieId.getData(),charset);
				}	
			}
			else if (booleanAdd)
			{	longMovieId++;
				stringMovieId=stringMovieFile+Long.toString(longMovieId);
				byteMovieId=stringMovieId.getBytes(charset); 
				dbMovieId=new DatabaseEntry(byteMovieId);
				bdbMovieId.put(null,dbMovie,dbMovieId);
			//	stringMovieIMDb=stringMovieId;
			}
//			stringMovieIMDb+="\t"+stringToAdd;
			
		//	System.out.println("Movie:"+stringMovie+" "+stringMovieIMDb);
			}
		catch(DatabaseException dbe)
		{	System.err.println("line222" + dbe);
			dbe.printStackTrace();
		}
		catch(Exception e)
		{	System.out.println("227"+e);
		}
		return stringMovieId;
	}
	public void updatebdbTvYear(String stringTv,String stringTvId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue,dbValueNew;
		byte[] byteKey, byteValueNew,byteValue,byteValueOld;
		byteKey=stringTv.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		dbValue=new DatabaseEntry();
		try
		{	byteValueNew=(stringTvId+stringToAdd).getBytes(charset);//(stringMovieId+"\t"+stringYear +"\t"+stringYearId).getBytes(charset);
			dbValueNew=new DatabaseEntry(byteValueNew);		
			bdbTvYear.put(null,dbKey,dbValueNew); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	}

	public String checkTv(String  stringMovie,Boolean booleanAdd)// String stringToAdd) //used stringMovie cause stringTv is derived from same matcher
	{	byte[] byteTv,byteTvId;
		DatabaseEntry dbTv, dbTvId;
		String stringTvIMDb="";
		byteTv=stringMovie.getBytes(charset);
		dbTv=new DatabaseEntry(byteTv);
		dbTvId=new DatabaseEntry();
		stringTvId="";
		String []stringValue;
		try
		{	if (bdbTvId.exists(null,dbTv)==OperationStatus.SUCCESS)
			{	if(bdbTvId.get(null,dbTv,dbTvId,null)==OperationStatus.SUCCESS)
				{//	System.out.println("found from bdbTv");
					stringTvId=new String(dbTvId.getData(),charset);
				}	
			}
			else if (booleanAdd)
			{//	System.out.println("bdtv not exists"+stringMovie);
				longTvId++;
				stringTvId=stringTvFile + Long.toString(longTvId);
			//	stringTvIMDb=stringTvId;
				byteTvId=stringTvId.getBytes(charset);
				dbTvId=new DatabaseEntry(byteTvId);
				bdbTvId.put(null,dbTv,dbTvId);
			}
		}
		catch(DatabaseException dbe)
		{	System.err.println("257"+dbe);
			dbe.printStackTrace();
		}
		catch(Exception e)
		{	System.out.println("261"+e);
		}
		return stringTvId;
	}
	public void updatebdbEpisodeYear(String stringEpisode, String stringEpisodeId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue,dbValueNew;
		byte[] byteKey, byteValueNew,byteValue,byteValueOld;
		byteKey=stringEpisode.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		dbValue=new DatabaseEntry();
		try
		{	byteValueNew=(stringEpisodeId+stringToAdd).getBytes(charset);//(stringMovieId+"\t"+stringYear +"\t"+stringYearId).getBytes(charset);
			dbValueNew=new DatabaseEntry(byteValueNew);
			bdbEpisodeYear.put(null,dbKey,dbValueNew);
		 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	}

	public void updatebdbEpisodeTv(String stringEpisode, String stringEpisodeId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue,dbValueNew;
		byte[] byteKey, byteValueNew,byteValue,byteValueOld;
		byteKey=stringEpisode.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		dbValue=new DatabaseEntry();
		try
		{	byteValueNew=(stringEpisodeId+stringToAdd).getBytes(charset);//(stringMovieId+"\t"+stringYear +"\t"+stringYearId).getBytes(charset);
			dbValueNew=new DatabaseEntry(byteValueNew);
			bdbEpisodeTv.put(null,dbKey,dbValueNew);
		 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	}

	public void updatebdbTvEpisode(String stringTv, String stringTvId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue,dbValueNew;
		byte[] byteKey, byteValueNew,byteValue,byteValueOld;
		byteKey=stringTv.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		dbValue=new DatabaseEntry();
		try
		{	byteValueNew=(stringTvId+stringToAdd).getBytes(charset);//(stringMovieId+"\t"+stringYear +"\t"+stringYearId).getBytes(charset);
			dbValueNew=new DatabaseEntry(byteValueNew);
			bdbTvEpisode.put(null,dbKey,dbValueNew);
		 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	}
	
	public String checkEpisode(String stringEpisode,Boolean booleanAdd) //, String stringToAdd)
	{	byte[] byteEpisode,byteEpisodeId;
		DatabaseEntry dbEpisode,dbEpisodeId;
		String stringEpisodeIMDb="";
		byteEpisode=stringEpisode.getBytes(charset);
		dbEpisode=new DatabaseEntry(byteEpisode);
		dbEpisodeId=new DatabaseEntry();
		stringEpisodeId="";
		String [] stringValue;
		try
		{	if(bdbEpisodeId.exists(null,dbEpisode)==OperationStatus.SUCCESS)
			{	if(bdbEpisodeId.get(null,dbEpisode,dbEpisodeId,null)==OperationStatus.SUCCESS)
				{	stringEpisodeId=new String(dbEpisodeId.getData(),charset);
				}
			}
			else if (booleanAdd)
			{	longEpisodeId++;
				stringEpisodeId=stringEpisodeFile +Long.toString(longEpisodeId);
				//stringEpisodeIMDb=stringEpisodeId;
				byteEpisodeId=stringEpisodeId.getBytes(charset);
				dbEpisodeId=new DatabaseEntry(byteEpisodeId);
				bdbEpisodeId.put(null,dbEpisode,dbEpisodeId);
			}
			//stringEpisodeIMDb +=" "+ stringToAdd;
			
		//	System.out.println("episode:"+stringEpisode+" "+stringEpisodeIMDb+ " stringtoAdd:"+stringToAdd);
		}
		catch(DatabaseException dbe)
		{	System.err.println(dbe);
			dbe.printStackTrace();
		}
		catch(Exception e)
		{	System.out.println("298"+e);
		}
		return stringEpisodeId;
	}
	
	public void readMovies(String sourceFile)//,String destFile) //SourceFile to read the contents from
	{
		Path pathSourceFile=Paths.get(sourceFile);
		//Path pathDestFile=Paths.get(destFile);
	
		Pattern patternYear;
		patternYear = Pattern.compile("[(][0-9?]{4}");//for matching 1st year
		Matcher matcherYear;
		Pattern patternEpisode;
		patternEpisode= Pattern.compile("[{].*[}]"); // for matching episode
		Matcher matcherEpisode;
		Pattern patternEpisodeYear;
		patternEpisodeYear=Pattern.compile("[0-9?]{4}");
		Matcher matcherEpisodeYear;
		
		stringFile=mapFiles.get("year");
		String stringtemp="";
		String stringPrevTv="",stringPrevTvYear="";
		DatabaseEntry dbKey, dbValue,dbValueNew;
		byte[] byteKey, byteValueNew,byteValue,byteValueOld;
		try
		{	
			bdbYearMovie=new Database("graphsHash/yearMovie.db",null,dbConfig);
			bdbYearEpisode=new Database("graphsHash/yearEpisode.db",null,dbConfig);
			bdbYearTv=new Database("graphsHash/yearTv.db",null,dbConfig);
			bdbMovieYear=new Database("graphsHash/movieYear.db",null,dbConfig);
			bdbEpisodeYear=new Database("graphsHash/episodeYear.db",null,dbConfig);
			bdbTvYear=new Database("graphsHash/tvYear.db",null,dbConfig);
			bdbTvEpisode=new Database("graphsHash/tvEpisode.db",null,dbConfig);
			bdbEpisodeTv=new Database("graphsHash/episodeTv.db",null,dbConfig);
			
			
		}
		catch(Exception dbe)
		{	System.err.println(dbe);
			dbe.printStackTrace();
		}
		try
		{	reader=Files.newBufferedReader(pathSourceFile,charset);
			while ((line = reader.readLine()) != null)
			{//	System.out.println(line);
				matcherYear = patternYear.matcher(line);
				totalRows++;
				try
				{	
					if (matcherYear.find())
					{	matchPositionYear=matcherYear.start();
						stringMovie=line.substring(0,matcherYear.start()-1).trim();//.replaceAll("\"","");
						stringYear=matcherYear.group().substring(1,5);
						//store in bdb
						
						stringYearId=checkYear(stringYear,true); //dbYear);
						if(stringYearId!="")	
						
						if (stringMovie.charAt(0)!='\"')  //its a movie since " " missing
						{	
							stringYearId=checkYear(stringYear,true);//,"");//,stringMovie);
							stringMovieId=checkMovie(stringMovie,true);//,"");//+"-"+stringYear);
							updatebdbMovieYear(stringMovie, stringMovieId,"\t"+stringYear+"\t"+stringYearId);
							updatebdbYearMovie(stringYear, stringYearId , "\t"+stringMovie + "\t"+stringMovieId);
						}
						else  //its a TV series
						{	
							if (stringTvId!="" && !(stringPrevTv.equals(stringMovie) && stringPrevTvYear.equals(stringYear)))
							{stringTvId=checkTv(stringMovie,true);//,"");//stringYearId+"-"+stringYear);
							stringYearId=checkYear(stringYear,true);//,stringTvId+"-"+stringMovie);
							updatebdbTvYear(stringMovie,stringTvId, "\t"+stringYear+"\t"+  stringYearId);
							updatebdbYearTv(stringYear, stringYearId,"\t"+stringMovie+ "\t"+ stringTvId);
						
							stringPrevTv=stringMovie;
							stringPrevTvYear=stringYear;
							}							
							matcherEpisode=patternEpisode.matcher(line);
							try
							{	if (matcherEpisode.find())
								{	matcherEpisodeYear=patternEpisodeYear.matcher(line);
									if(matcherEpisodeYear.find(matcherEpisode.end()))
									{//stringYear= (line.substring(matcherEpisode.end())).trim();
										stringYear=matcherEpisodeYear.group();
										
										stringEpisode=((matcherEpisode.group()).replaceAll("\\{","")).replaceAll("\\}","");
										
										stringYearId=checkYear(stringYear,true);//,"");
										stringEpisodeId=checkEpisode(stringEpisode,true);//,"");// stringYearId);//+"-"+stringYear);
										stringYearId=checkYear(stringYear,true);//stringEpisodeId);//+"-"+stringEpisode);

										updatebdbEpisodeYear(stringEpisode,stringEpisodeId, "\t"+stringYear +"\t"+stringYearId);
										updatebdbYearEpisode(stringYear,stringYearId, "\t"+stringEpisode +"\t"+stringEpisodeId);
										updatebdbTvEpisode(stringMovie, stringTvId, "\t"+stringEpisode+"\t"+stringEpisodeId);
										updatebdbEpisodeTv(stringEpisode,stringEpisodeId, "\t"+stringMovie+"\t"+stringTvId);
										
									
									}
									else
									{	episodeYearErrorRows++;
									}
								}
								else
								{	episodeErrorRows++;
								}
							}	
							
							catch(IllegalStateException ise)
							{	System.err.println(ise);
								ise.printStackTrace();
							}
						}
					}
					else
					{	movieErrorRows++;
					}
				}
				catch(IllegalStateException ise)
				{	System.out.println("Error at line 194"+ise);
				}
				catch(PatternSyntaxException pse)
				{	System.err.println("Error at line 197"+pse);
				}			
						
			}
			reader.close();
			writer=Files.newBufferedWriter(Paths.get("graphsHash/yearTv.txt"),charset);
			writeDb(bdbYearTv,writer);
			bdbYearTv.close();
			writer.close();
			writer=Files.newBufferedWriter(Paths.get("graphsHash/yearEpisode.txt"),charset);
			writeDb(bdbYearEpisode,writer);
			bdbYearEpisode.close();
			writer.close();
			writer=Files.newBufferedWriter(Paths.get("graphsHash/yearMovie.txt"),charset);
			writeDb(bdbYearMovie,writer);
			bdbYearMovie.close();
			writer.close();
			writer=Files.newBufferedWriter(Paths.get("graphsHash/movieYear.txt"),charset);
			writeDb(bdbMovieYear, writer);
			bdbMovieYear.close();
			writer.close();
			writer=Files.newBufferedWriter(Paths.get("graphsHash/tvYear.txt"),charset);
			writeDb(bdbTvYear, writer);
			bdbTvYear.close();
			writer.close();
			writer=Files.newBufferedWriter(Paths.get("graphsHash/episodeYear.txt"),charset);
			writeDb(bdbEpisodeYear, writer);
			bdbEpisodeYear.close();
			writer.close();
		
		}
		catch (IOException e)
		{	System.err.println("error at line 75"+e);
		}		  
		catch(DatabaseException dbe)
		{	System.err.println(dbe);
			dbe.printStackTrace();
		}
	
	}
//   --------------genre begin-----------
	public void updatebdbGenreTv(String stringGenre, String stringGenreId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue,dbValueNew;
		byte[] byteKey, byteValueNew,byteValue,byteValueOld;
		byteKey=stringGenre.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		dbValue=new DatabaseEntry();
		try
		{   byteValueNew=(stringGenreId+stringToAdd).getBytes(charset);//(stringMovieId+"\t"+stringYear +"\t"+stringYearId).getBytes(charset);
			dbValueNew=new DatabaseEntry(byteValueNew);
			bdbGenreTv.put(null,dbKey,dbValueNew); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	}
	public void updatebdbGenreEpisode(String stringGenre, String stringGenreId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue,dbValueNew;
		byte[] byteKey, byteValueNew,byteValue,byteValueOld;
		byteKey=stringGenre.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		dbValue=new DatabaseEntry();
		try
		{   byteValueNew=(stringGenreId+stringToAdd).getBytes(charset);//(stringMovieId+"\t"+stringYear +"\t"+stringYearId).getBytes(charset);
			dbValueNew=new DatabaseEntry(byteValueNew);
			bdbGenreEpisode.put(null,dbKey,dbValueNew); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	}
	public void updatebdbGenreMovie(String stringGenre, String stringGenreId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue,dbValueNew;
		byte[] byteKey, byteValueNew,byteValue,byteValueOld;
		byteKey=stringGenre.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		dbValue=new DatabaseEntry();
		try
		{   byteValueNew=(stringGenreId+stringToAdd).getBytes(charset);//(stringMovieId+"\t"+stringYear +"\t"+stringYearId).getBytes(charset);
			dbValueNew=new DatabaseEntry(byteValueNew);
			bdbGenreMovie.put(null,dbKey,dbValueNew); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	}

	public void updatebdbMovieGenre(String stringMovie, String stringMovieId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue,dbValueNew;
		byte[] byteKey, byteValueNew,byteValue,byteValueOld;
		byteKey=stringMovie.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		dbValue=new DatabaseEntry();
		try
		{   byteValueNew=(stringMovieId+stringToAdd).getBytes(charset);//(stringMovieId+"\t"+stringYear +"\t"+stringYearId).getBytes(charset);
			dbValueNew=new DatabaseEntry(byteValueNew);
			bdbMovieGenre.put(null,dbKey,dbValueNew); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	}
	
	public void updatebdbEpisodeGenre(String stringEpisode, String stringEpisodeId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue,dbValueNew;
		byte[] byteKey, byteValueNew,byteValue,byteValueOld;
		byteKey=stringEpisode.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		dbValue=new DatabaseEntry();
		try
		{   byteValueNew=(stringEpisodeId+stringToAdd).getBytes(charset);//(stringMovieId+"\t"+stringYear +"\t"+stringYearId).getBytes(charset);
			dbValueNew=new DatabaseEntry(byteValueNew);
			bdbEpisodeGenre.put(null,dbKey,dbValueNew); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	}
	
	public void updatebdbTvGenre(String stringTv, String stringTvId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue,dbValueNew;
		byte[] byteKey, byteValueNew,byteValue,byteValueOld;
		byteKey=stringTv.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		dbValue=new DatabaseEntry();
		try
		{   byteValueNew=(stringTvId+stringToAdd).getBytes(charset);//(stringMovieId+"\t"+stringYear +"\t"+stringYearId).getBytes(charset);
			dbValueNew=new DatabaseEntry(byteValueNew);
			bdbTvGenre.put(null,dbKey,dbValueNew); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	}
	


	public String checkGenre(String stringGenre,Boolean booleanAdd)
	{	byte[] byteGenre,byteGenreId;
		DatabaseEntry dbGenre,dbGenreId;
		String stringGenreIMDb;
		byteGenre=stringGenre.getBytes(charset);
		dbGenre=new DatabaseEntry(byteGenre);
		dbGenreId=new DatabaseEntry();
		String stringGenreId="";
		try
		{	if(bdbGenreId.exists(null,dbGenre)==OperationStatus.SUCCESS)
			{	if(bdbGenreId.get(null,dbGenre,dbGenreId,null)==OperationStatus.SUCCESS)
				{	stringGenreId=new String(dbGenreId.getData(),charset);
					//return stringGenreId;
				}
			}
			else if (booleanAdd)
			{	longGenreId++;
				stringGenreId=stringFile+Long.toString(longGenreId);
				byteGenreId=stringGenreId.getBytes(charset);
				dbGenreId=new DatabaseEntry(byteGenreId);
				bdbGenreId.put(null,dbGenre,dbGenreId);
			}
		}
		catch(DatabaseException dbe)
		{	System.err.println(dbe);
			dbe.printStackTrace();
		}
		return stringGenreId;
	}

	public void readGenre(String sourceFile)//,String destFile) //SourceFile to read the contents from
	{
		Path pathSourceFile=Paths.get(sourceFile);
		//Path pathDestFile=Paths.get(destFile);
	
		Pattern patternYear;
		patternYear = Pattern.compile("[(][0-9?]{4}");//for matching 1st year
		Matcher matcherYear;
		Pattern patternEpisode;
		patternEpisode= Pattern.compile("[{].*[}]"); // for matching episode
		Matcher matcherEpisode;
		Pattern patternEpisodeYear;
		//patternEpisodeYear=Pattern.compile("[0-9?]{4}");
		//Matcher matcherEpisodeYear;
		Matcher matcherGenre;
		Pattern patternGenre=Pattern.compile("[A-Za-z-]*$");
		String stringGenreId="",stringGenre="",stringGenreIMDb="";
		
		String stringPrevGenre="", stringPrevTv="";
		String []lines;
		stringFile=mapFiles.get("genre");
		
		try
		{	bdbGenreId=new Database("graphsHash/genreId.db",null,dbConfig);
			bdbGenreTv=new Database("graphsHash/genreTv.db",null,dbConfig);
			bdbGenreEpisode=new Database("graphsHash/genreEpisode.db",null,dbConfig);
			bdbGenreMovie=new Database("graphsHash/genreMovie.db",null,dbConfig);
			bdbTvGenre=new Database("graphsHash/tvGenre.db",null,dbConfig);
			bdbEpisodeGenre=new Database("graphsHash/episodeGenre.db",null,dbConfig);
			bdbMovieGenre=new Database("graphsHash/movieGenre.db",null,dbConfig);
			
		}
		catch(Exception dbe)
		{	System.err.println(dbe);
			dbe.printStackTrace();
		}
		
		try
		{	reader=Files.newBufferedReader(pathSourceFile,charset);
			//writer=Files.newBufferedWriter(pathDestFile,charset);
			while ((line = reader.readLine()) != null)
			{	//System.out.println("line:"+line);
				matcherYear = patternYear.matcher(line);
				totalRows++;
				try
				{	
					if (matcherYear.find())
					{	matchPositionYear=matcherYear.start();
						stringMovie=line.substring(0,matcherYear.start()-1).trim();//.replaceAll("\"","");
						//store in bdb
						matcherGenre=patternGenre.matcher(line);
						if (matcherGenre.find(matcherYear.end()))
						{stringGenre=(matcherGenre.group()).trim();
						stringGenreId=checkGenre(stringGenre,true);
						//System.out.println("stringGenre:"+stringGenre);
						
						if (stringMovie.charAt(0)!='\"')  //its a movie since " " missing
						{	
							//store Type1, MovieId, MovieName, Type2, Year Id, Year , Relation - into a file
							stringMovieId=checkMovie(stringMovie,true);
							updatebdbGenreMovie(stringGenre, stringGenreId,stringMovie+"\t"+stringMovieId);
							updatebdbMovieGenre(stringMovie, stringMovieId,stringGenre+"\t"+stringGenreId);
							
						}
						else  //its a TV series
						{	stringEpisode=""; // to enable isempty check
							matcherEpisode=patternEpisode.matcher(line);
							try
							{	if (matcherEpisode.find())
								{	stringEpisode=((matcherEpisode.group()).replaceAll("\\{","")).replaceAll("\\}","");
									stringEpisodeId=checkEpisode(stringEpisode,true);
									matcherGenre=patternGenre.matcher(line);
									if(matcherGenre.find(matcherEpisode.end()+1))
									{stringGenre=(matcherGenre.group()).trim();
									stringGenreId=checkGenre(stringGenre,true);
									updatebdbGenreEpisode(stringGenre, stringGenreId,stringEpisode+"\t"+stringEpisodeId);
									updatebdbEpisodeGenre(stringEpisode, stringEpisodeId,stringGenre+"\t"+stringGenreId);
								}
								}
							}
							catch(IllegalStateException ise)
							{	System.err.println(ise);
								ise.printStackTrace();
							}
							if ((!stringPrevTv.equals(stringMovie)) || (stringEpisode.isEmpty()))  // only if not a repetition
							{	stringTvId=checkTv(stringMovie,true);
								updatebdbGenreTv(stringGenre, stringGenreId,"\t"+stringMovie+"\t"+stringTvId);
								updatebdbTvGenre(stringMovie, stringTvId,"\t"+stringGenre+"\t"+stringGenreId);
							}
						}	
							
						}
						else
						{	movieErrorRows++;
						}	
					}
					else
					{	movieErrorRows++;
					}
				}
				catch(IllegalStateException ise)
				{	System.out.println("Error at line 194"+ise);
				}
				catch(PatternSyntaxException pse)
				{	System.err.println("Error at line 197"+pse);
				}	
				stringPrevTv=stringMovie;
			}
			reader.close();
			
			writer=Files.newBufferedWriter(Paths.get("graphsHash/genreTv.txt"),charset);
			writeDb(bdbGenreTv,writer);
			bdbGenreTv.close();
			writer.close();
			writer=Files.newBufferedWriter(Paths.get("graphsHash/genreEpisode.txt"),charset);
			writeDb(bdbGenreEpisode,writer);
			bdbGenreEpisode.close();
			writer.close();
			writer=Files.newBufferedWriter(Paths.get("graphsHash/genreMovie.txt"),charset);
			writeDb(bdbGenreMovie,writer);
			bdbGenreMovie.close();
			writer.close();
			writer=Files.newBufferedWriter(Paths.get("graphsHash/genreId.txt"),charset);
			writeDb(bdbGenreId,writer);
			bdbGenreId.close();
			writer.close();
			
			writer=Files.newBufferedWriter(Paths.get("graphsHash/movieGenre.txt"),charset);
			writeDb(bdbMovieGenre,writer);
			bdbMovieGenre.close();
			writer.close();
			writer=Files.newBufferedWriter(Paths.get("graphsHash/tvGenre.txt"),charset);
			writeDb(bdbTvGenre,writer);
			bdbTvGenre.close();
			writer.close();
			writer=Files.newBufferedWriter(Paths.get("graphsHash/episodeGenre.txt"),charset);
			writeDb(bdbEpisodeGenre,writer);
			bdbEpisodeGenre.close();
			writer.close();
			
		}
		catch (IOException e)
		{	System.err.println("error at line 75"+e);
		}		  
		catch(DatabaseException dbe)
		{	System.err.println(dbe);
			dbe.printStackTrace();
		}
	}


// ---------------check genre end ------------

// --------------checklanguage begin-----------
	public void updatebdbLanguageMovie(String stringLanguage, String stringLanguageId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue,dbValueNew;
		byte[] byteKey, byteValueNew,byteValue,byteValueOld;
		byteKey=stringLanguage.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		dbValue=new DatabaseEntry();
		try
		{   byteValueNew=(stringLanguageId+stringToAdd).getBytes(charset);//(stringMovieId+"\t"+stringYear +"\t"+stringYearId).getBytes(charset);
			dbValueNew=new DatabaseEntry(byteValueNew);
			bdbLanguageMovie.put(null,dbKey,dbValueNew); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	}

	public void updatebdbLanguageTv(String stringLanguage, String stringLanguageId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue,dbValueNew;
		byte[] byteKey, byteValueNew,byteValue,byteValueOld;
		byteKey=stringLanguage.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		dbValue=new DatabaseEntry();
		try
		{   byteValueNew=(stringLanguageId+stringToAdd).getBytes(charset);//(stringMovieId+"\t"+stringYear +"\t"+stringYearId).getBytes(charset);
			dbValueNew=new DatabaseEntry(byteValueNew);
			bdbLanguageTv.put(null,dbKey,dbValueNew); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	}
	public void updatebdbLanguageEpisode(String stringLanguage, String stringLanguageId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue,dbValueNew;
		byte[] byteKey, byteValueNew,byteValue,byteValueOld;
		byteKey=stringLanguage.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		dbValue=new DatabaseEntry();
		try
		{   byteValueNew=(stringLanguageId+stringToAdd).getBytes(charset);//(stringMovieId+"\t"+stringYear +"\t"+stringYearId).getBytes(charset);
			dbValueNew=new DatabaseEntry(byteValueNew);
			bdbLanguageEpisode.put(null,dbKey,dbValueNew); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	}

	public void updatebdbEpisodeLanguage(String stringEpisode, String stringEpisodeId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue,dbValueNew;
		byte[] byteKey, byteValueNew,byteValue,byteValueOld;
		byteKey=stringEpisode.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		dbValue=new DatabaseEntry();
		try
		{   byteValueNew=(stringEpisodeId+stringToAdd).getBytes(charset);//(stringMovieId+"\t"+stringYear +"\t"+stringYearId).getBytes(charset);
			dbValueNew=new DatabaseEntry(byteValueNew);
			bdbEpisodeLanguage.put(null,dbKey,dbValueNew); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	}

	public void updatebdbMovieLanguage(String stringMovie, String stringMovieId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue,dbValueNew;
		byte[] byteKey, byteValueNew,byteValue,byteValueOld;
		byteKey=stringMovie.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		dbValue=new DatabaseEntry();
		try
		{   byteValueNew=(stringEpisodeId+stringToAdd).getBytes(charset);//(stringMovieId+"\t"+stringYear +"\t"+stringYearId).getBytes(charset);
			dbValueNew=new DatabaseEntry(byteValueNew);
			bdbMovieLanguage.put(null,dbKey,dbValueNew); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	}
	public void updatebdbTvLanguage(String stringTv, String stringTvId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue,dbValueNew;
		byte[] byteKey, byteValueNew,byteValue,byteValueOld;
		byteKey=stringTv.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		dbValue=new DatabaseEntry();
		try
		{   byteValueNew=(stringTvId+stringToAdd).getBytes(charset);//(stringMovieId+"\t"+stringYear +"\t"+stringYearId).getBytes(charset);
			dbValueNew=new DatabaseEntry(byteValueNew);
			bdbTvLanguage.put(null,dbKey,dbValueNew); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	}

	public String checkLanguage(String stringLanguage,Boolean booleanAdd)
	{	byte[] byteLanguage,byteLanguageId;
		DatabaseEntry dbLanguage,dbLanguageId;
		String stringLanguageIMDb;
		byteLanguage=stringLanguage.getBytes(charset);
		dbLanguage=new DatabaseEntry(byteLanguage);
		dbLanguageId=new DatabaseEntry();
		String stringLanguageId="";
		try
		{	if(bdbLanguageId.exists(null,dbLanguage)==OperationStatus.SUCCESS)
			{	if(bdbLanguageId.get(null,dbLanguage,dbLanguageId,null)==OperationStatus.SUCCESS)
				{	stringLanguageId=new String(dbLanguageId.getData(),charset);
					//return stringLanguageId;
				}
			}
			else if (booleanAdd)
			{	longLanguageId++;
				stringLanguageId=stringFile+Long.toString(longLanguageId);
				byteLanguageId=stringLanguageId.getBytes(charset);
				dbLanguageId=new DatabaseEntry(byteLanguageId);
				bdbLanguageId.put(null,dbLanguage,dbLanguageId);
				stringLanguageIMDb=stringLanguageId+"\t"+stringLanguage+"\n";
			}
		}
		catch(DatabaseException dbe)
		{	System.err.println(dbe);
			dbe.printStackTrace();
		}
		return stringLanguageId;
	}

	public void readLanguage(String sourceFile)//,String destFile) //SourceFile to read the contents from
	{
		Path pathSourceFile=Paths.get(sourceFile);
		//Path pathDestFile=Paths.get(destFile);
	
		Pattern patternYear;
		patternYear = Pattern.compile("[(][0-9?]{4}");//for matching 1st year
		Matcher matcherYear;
		Pattern patternEpisode;
		patternEpisode= Pattern.compile("[{].*[}]"); // for matching episode
		Matcher matcherEpisode;
		Pattern patternLanguage=Pattern.compile("\t.*$");
		Matcher matcherLanguage;
		//Pattern patternLanguageEdge=Patter.compile("[(].[)]");
		//Matcher matcherLanguageEdge;
		String stringLanguageId="",stringLanguage="",stringLanguageIMDb="";
		String stringLanguageEdge=""; //to store the (english subtitles)
		int intLanguageEdgePos=0;
		String stringPrevLanguage="", stringPrevTv="", stringPrevEpisode="";
		String []lines;
		stringFile=mapFiles.get("language");
		
		try
		{
			bdbLanguageId=new Database("graphsHash/languageId.db",null,dbConfig);
			bdbLanguageTv=new Database("graphsHash/languageTv.db",null,dbConfig);
			bdbLanguageEpisode=new Database("graphsHash/languageEpisode.db",null,dbConfig);
			bdbLanguageMovie=new Database("graphsHash/languageMovie.db",null,dbConfig);
			bdbMovieLanguage=new Database("graphsHash/movieLanguage.db",null,dbConfig);
			bdbEpisodeLanguage=new Database("graphsHash/episodeLanguage.db",null,dbConfig);
			bdbTvLanguage=new Database("graphsHash/tvLanguage.db",null,dbConfig);
		}
		catch(Exception dbe)
		{	System.err.println(dbe);
			dbe.printStackTrace();
		}
		
		try
		{	reader=Files.newBufferedReader(pathSourceFile,charset);
			//writer=Files.newBufferedWriter(pathDestFile,charset);
			while ((line = reader.readLine()) != null)
			{	//System.out.println("line:"+line);
				matcherYear = patternYear.matcher(line);
				totalRows++;
				try
				{	
					if (matcherYear.find())
					{	matchPositionYear=matcherYear.start();
						stringMovie=line.substring(0,matcherYear.start()-1).trim();//.replaceAll("\"","");
						//store in bdb
						
						matcherLanguage=patternLanguage.matcher(line);
						if (matcherLanguage.find())
						{	stringLanguage=(matcherLanguage.group()).trim();
							intLanguageEdgePos=stringLanguage.indexOf("(");
							if (intLanguageEdgePos>0 && stringLanguage.indexOf(")")>0) //additional information after language eg. song lyrics, original version
							{	stringLanguageEdge="language "+(stringLanguage.substring(intLanguageEdgePos+1,stringLanguage.length()-1)).trim();
								stringLanguage=(stringLanguage.substring(0,intLanguageEdgePos)).trim();
							}
							else	stringLanguageEdge="language";
							stringLanguageId=checkLanguage(stringLanguage,true);
						}	
						if (stringMovie.charAt(0)!='\"')  //its a movie since " " missing
						{	
							//store Type1, MovieId, MovieName, Type2, Year Id, Year , Relation - into a file
							stringMovieId=checkMovie(stringMovie,true);
							updatebdbLanguageMovie(stringLanguage,stringLanguageId,"\t"+stringMovie+"\t"+stringMovieId);
							updatebdbMovieLanguage(stringMovie,stringMovieId,"\t"+stringLanguage+"\t"+stringLanguageId);
						//	if (stringMovieId!="")
						}
						else  //its a TV series
						{	stringEpisode=""; // to enable isempty check
							matcherEpisode=patternEpisode.matcher(line);
							
							try
							{	if (matcherEpisode.find())
								{	stringEpisode=((matcherEpisode.group()).replaceAll("\\{","")).replaceAll("\\}","");
									stringEpisodeId=checkEpisode(stringEpisode,true);
									updatebdbLanguageEpisode(stringLanguage,stringLanguageId,"\t"+stringEpisode+"\t"+stringEpisodeId);
									updatebdbEpisodeLanguage(stringEpisode,stringMovieId,"\t"+stringLanguage+"\t"+stringLanguageId);
								}
							}
							catch(IllegalStateException ise)
							{	System.err.println(ise);
								ise.printStackTrace();
							}
							if (!(stringPrevTv.equals(stringMovie)) || (stringEpisode.isEmpty()))  // only if not a repeatition
							{	stringTvId=checkTv(stringMovie,true);
								updatebdbLanguageTv(stringLanguage,stringLanguageId,"\t"+stringMovie+"\t"+stringTvId);
								updatebdbTvLanguage(stringMovie,stringTvId,"\t"+stringLanguage+"\t"+stringLanguageId);
							}
						}	
							
					}
					else
					{	movieErrorRows++;
					}
				}
				catch(IllegalStateException ise)
				{	System.err.println("Error at line 194"+ise);
					ise.printStackTrace();
				}
				catch(PatternSyntaxException pse)
				{	System.err.println("Error at line 197"+pse);
					pse.printStackTrace();
				}	
				stringPrevTv=stringMovie;
				stringPrevEpisode=stringEpisode;
			}
			reader.close();
			writer=Files.newBufferedWriter(Paths.get("graphsHash/languageTv.txt"),charset);
			writeDb(bdbLanguageTv,writer);
			bdbLanguageTv.close();
			writer.close();
			writer=Files.newBufferedWriter(Paths.get("graphsHash/languageEpisode.txt"),charset);
			writeDb(bdbLanguageEpisode,writer);
			bdbLanguageEpisode.close();
			writer.close();
			writer=Files.newBufferedWriter(Paths.get("graphsHash/languageMovie.txt"),charset);
			writeDb(bdbLanguageMovie,writer);
			bdbLanguageMovie.close();
			writer.close();
			writer=Files.newBufferedWriter(Paths.get("graphsHash/languageId.txt"),charset);
			writeDb(bdbLanguageId,writer);
			bdbLanguageId.close();
			writer.close();
			
			writer=Files.newBufferedWriter(Paths.get("graphsHash/movieLanguage.txt"),charset);
			writeDb(bdbMovieLanguage,writer);
			bdbMovieLanguage.close();
			writer.close();
			writer=Files.newBufferedWriter(Paths.get("graphsHash/episodeLanguage.txt"),charset);
			writeDb(bdbEpisodeLanguage,writer);
			bdbEpisodeLanguage.close();
			writer.close();
			writer=Files.newBufferedWriter(Paths.get("graphsHash/tvLanguage.txt"),charset);
			writeDb(bdbTvLanguage,writer);
			bdbTvLanguage.close();
			writer.close();
			
		}
		catch (IOException e)
		{	System.err.println("error at line 75"+e);
		}		  
		catch(DatabaseException dbe)
		{	System.err.println(dbe);
			dbe.printStackTrace();
		}
	}
// ------------	readLanguage end ------------
	
	public String checkPerson(String stringPerson, Boolean booleanAdd)
	{	DatabaseEntry dbPerson, dbPersonId;
		byte[] bytePerson, bytePersonId;
		//String stringActorIMDb;
		bytePerson=stringPerson.getBytes(charset);
		dbPerson=new DatabaseEntry(bytePerson);	
		dbPersonId=new DatabaseEntry();
		String stringPersonId="";
		try
		{
			if(bdbPersonId.exists(null,dbPerson)==OperationStatus.SUCCESS)
			{	if(bdbPersonId.get(null,dbPerson,dbPersonId,null)==OperationStatus.SUCCESS)
				{	stringPersonId=new String(dbPersonId.getData(),charset);
					//return stringActorId;
				}
			}
			else if (booleanAdd)
			{	longPersonId++;
				stringPersonId=stringPersonFile+Long.toString(longPersonId);
				bytePersonId=stringPersonId.getBytes(charset);//(Long.toString(yearId)).getBytes();
				dbPersonId=new DatabaseEntry(bytePersonId);
				bdbPersonId.put(null,dbPerson,dbPersonId);
			}
		}
		catch(DatabaseException dbe)
		{	System.err.println(dbe);
			dbe.printStackTrace();
		}
		return stringPersonId;
	}
	public void updatebdbPerson(String stringPerson, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue;
		byte[] byteKey,byteValue;
		byteKey=stringPerson.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		dbValue=new DatabaseEntry();
		String stringPersonId;
		try
		{   if (bdbPerson.exists(null,dbKey)==OperationStatus.SUCCESS)
			{	if (bdbPerson.get(null,dbKey,dbValue,null)==OperationStatus.SUCCESS)
				{ 	stringPersonId=new String(dbValue.getData(),charset);
					dbValue=new DatabaseEntry((stringPersonId+ "\t"+stringToAdd).getBytes(charset));
					bdbPerson.delete(null,dbKey);
					bdbPerson.put(null,dbKey,dbValue);
				}
			}
			else
			{	longPersonId++;
				stringPersonId= stringPersonFile+Long.toString(longPersonId);
				byteValue=(stringPersonId+stringToAdd).getBytes(charset);//(stringMovieId+"\t"+stringYear +"\t"+stringYearId).getBytes(charset);
				dbValue=new DatabaseEntry(byteValue);
				bdbPerson.put(null,dbKey,dbValue); 
			}
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	}
	public void updatebdbActorTv(String stringActor, String stringActorId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue;
		byte[] byteKey,byteValue;
		byteKey=stringActor.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		byteValue=(stringActorId+stringToAdd).getBytes(charset);//(stringMovieId+"\t"+stringYear +"\t"+stringYearId).getBytes(charset);
		dbValue=new DatabaseEntry(byteValue);
			
		try
		{   bdbActorTv.put(null,dbKey,dbValue); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	}

	public void updatebdbActorMovie(String stringActor, String stringActorId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue, dbValueOld;
		byte[] byteKey,byteValue;
		byteKey=stringActor.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		byteValue=(stringActorId+stringToAdd).getBytes(charset);
		dbValue=new DatabaseEntry(byteValue);
		
		try
		{   bdbActorMovie.put(null,dbKey,dbValue); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	}

	public void updatebdbActorEpisode(String stringActor, String stringActorId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue;
		byte[] byteKey,byteValue;
		byteKey=stringActor.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		byteValue=(stringActorId+stringToAdd).getBytes(charset);//(stringMovieId+"\t"+stringYear +"\t"+stringYearId).getBytes(charset);
		dbValue=new DatabaseEntry(byteValue);
			
		try
		{   bdbActorEpisode.put(null,dbKey,dbValue); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	}

	public void updatebdbMovieActor(String stringMovie, String stringMovieId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue;
		byte[] byteKey,byteValue;
		byteKey=stringMovie.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		byteValue=(stringMovieId+stringToAdd).getBytes(charset);
		dbValue=new DatabaseEntry(byteValue);
		try
		{   bdbMovieActor.put(null,dbKey,dbValue); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}

		}

	public void updatebdbTvActor(String stringTv, String stringTvId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue;
		byte[] byteKey,byteValue;
		byteKey=stringTv.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		byteValue=(stringTvId+stringToAdd).getBytes(charset);//(stringMovieId+"\t"+stringYear +"\t"+stringYearId).getBytes(charset);
		dbValue=new DatabaseEntry(byteValue);
			
		try
		{   bdbTvActor.put(null,dbKey,dbValue); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	}
	public void updatebdbEpisodeActor(String stringEpisode, String stringEpisodeId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue;
		byte[] byteKey,byteValue;
		byteKey=stringEpisode.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		byteValue=(stringEpisodeId+stringToAdd).getBytes(charset);//(stringMovieId+"\t"+stringYear +"\t"+stringYearId).getBytes(charset);
		dbValue=new DatabaseEntry(byteValue);
			
		try
		{   bdbEpisodeActor.put(null,dbKey,dbValue); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	}

	public String checkActor(String stringActor,Boolean booleanAdd)
	{	DatabaseEntry dbActor, dbActorId;
		byte[] byteActor,byteActorId;
		String stringActorIMDb;
		byteActor=stringActor.getBytes(charset);
		dbActor=new DatabaseEntry(byteActor);	
		dbActorId=new DatabaseEntry();
		String stringActorId="", stringPersonValue="";
		try
		{
			if(bdbActorId.exists(null,dbActor)==OperationStatus.SUCCESS)
			{	if(bdbActorId.get(null,dbActor,dbActorId,null)==OperationStatus.SUCCESS)
				{	stringActorId=new String(dbActorId.getData(),charset);
					//return stringActorId;
				}
			}
			else if (booleanAdd)
			{	longActorId++;longPersonId++;
				stringActorId=stringFile+Long.toString(longActorId);
				byteActorId=stringActorId.getBytes(charset);//(Long.toString(yearId)).getBytes();
				dbActorId=new DatabaseEntry(byteActorId);
				bdbActorId.put(null,dbActor,dbActorId);
				stringPersonValue=stringPersonFile+Long.toString(longPersonId)+"\t"+stringActorId;
				byteActorId=stringPersonValue.getBytes(charset);
				dbActorId=new DatabaseEntry(byteActorId);
				bdbPerson.put(null,dbActor,dbActorId);			

			}
		}
		catch(DatabaseException dbe)
		{	System.err.println(dbe);
			dbe.printStackTrace();
		}
		return stringActorId;
	}		

	public void readActorsEpisode(String sourceFile)//,String destFile) //SourceFile to read the contents from
	{
		Path pathSourceFile=Paths.get(sourceFile);
		//Path pathDestFile=Paths.get(destFile);//"graphs/movieGraphIMDb.txt");

		Pattern patternYear = Pattern.compile("[(][0-9?]{4}");//for matching 1st year
		Matcher matcherYear ;
		Pattern patternTV= Pattern.compile("[\"].*[\"]"); // for matching episode
		Matcher matcherTV;
		Pattern patternMovie=Pattern.compile("[\t].*[(]");
		Matcher matcherMovie; 
		Pattern patternEpisode=Pattern.compile("[{].*[}]");
		Matcher matcherEpisode;
		Pattern patternCharacter=Pattern.compile("\\[.*\\]");
		Matcher matcherCharacter;
		String stringActorId="",stringActor="",stringPersonId="";
		String stringCharacter="";
		String stringPrevActor="", stringPrevTv="";
		String []lines;
		String stringYear;
		stringFile=mapFiles.get("actor");
		
		try
		{
		
			if (bdbPerson==null) bdbPerson=new Database("graphsHash/person.db",null,dbConfig);
			if (bdbPersonId==null) bdbPersonId=new Database("graphsHash/personId.db",null,dbConfig);
			bdbActorId=new Database("graphsHash/actorId.db",null,dbConfig);
	
			bdbActorTv=new Database("graphsHash/actorTv.db",null,dbConfig);
			bdbActorEpisode=new Database("graphsHash/actorEpisode.db",null,dbConfig);
			bdbTvActor=new Database("graphsHash/tvActor.db",null,dbConfig);
			bdbEpisodeActor=new Database("graphsHash/episodeActor.db",null,dbConfig);			
		}
		catch(Exception dbe)
		{	System.err.println(dbe);
			dbe.printStackTrace();
		}
		
		
		try
		{	reader=Files.newBufferedReader(pathSourceFile,charset);
			int matchPositionYear=0,matchPositionEpisode=0; 
			long rowsMovies=0, rowsTotal=0,rowsAuthor=0,rowsTv=0;
			while ((line = reader.readLine()) != null)
			{	rowsTotal++;
				if (!(line.trim().isEmpty()))
				{	lines=line.split("\t");
					if (!(lines[0].trim().isEmpty()))
					{ 	stringActor=lines[0].trim();
						stringActorId=checkActor(stringActor,true);
						//stringPersonId=checkPerson(stringActor,true);
					}
					//System.out.println("stringActor:"+stringActor+" stringActorId:" +stringActorId);
					try
					{	matcherYear=patternYear.matcher(line);
						if (matcherYear.find())
						{	matchPositionYear=matcherYear.start();
							if (matchPositionYear>=lines[0].length())
							{	
								stringYear=matcherYear.group();
								stringMovie=(line.substring(lines[0].length(),matchPositionYear)).trim();//matcherYear.start())).trim();
								if (stringMovie.charAt(0)=='\"')  	//its a TV series
								{	try
									{	matcherCharacter=patternCharacter.matcher(line);
										if (matcherCharacter.find())
											stringCharacter=(matcherCharacter.group().replaceAll("\\[","")).replaceAll("\\]","");
										else
											stringCharacter="";
									}
									catch(IllegalStateException ise) { ise.printStackTrace();}
									
									matcherEpisode=patternEpisode.matcher(line);
									try
									{	if (matcherEpisode.find())
										{	stringEpisode=((matcherEpisode.group()).replaceAll("\\{","")).replaceAll("\\}","");
											try
											{	matcherCharacter=patternCharacter.matcher(line);
												if (matcherCharacter.find())
													stringCharacter=(matcherCharacter.group().replaceAll("\\[","")).replaceAll("\\]","");
												else
													stringCharacter="";
											}
											catch(IllegalStateException ise) { ise.printStackTrace();}
											
											stringEpisodeId=checkEpisode(stringEpisode,true);
											updatebdbEpisodeActor(stringEpisode,stringEpisodeId,"\t"+stringActor+"\t"+stringActorId+stringCharacter);
											updatebdbActorEpisode(stringActor,stringActorId,"\t"+stringEpisode+"\t"+stringEpisodeId+"\t"+stringYear+"\t"+stringCharacter);
									
										}
									}
									catch(IllegalStateException ise)
									{	System.err.println(ise);
										ise.printStackTrace();
									}
									if (!(stringPrevTv.equals(stringMovie)))  // only if not a repetition
									{
										stringTvId=checkTv(stringMovie,true);
										updatebdbTvActor(stringMovie,stringTvId,"\t"+stringYear+"\t"+stringActor+"\t"+stringActorId+"\t"+stringCharacter);
										updatebdbActorTv(stringActor,stringActorId,"\t"+stringMovie+"\t"+stringTvId+"\t"+stringYear+"\t"+stringCharacter);
									
									}
								}
							}
						}
					}
					catch(IllegalStateException ise)
					{	System.out.println("illegal state exception"+line);
					}
				}
			}
			System.out.println("Finished actors.list");
			reader.close();
/*			writerActorId=Files.newBufferedWriter(Paths.get("graphsHash/actorId.txt"),charset);
			writeDb(bdbActorId,writerActorId);
			bdbActorId.close();
			writerActorId.close();
*/			writer=Files.newBufferedWriter(Paths.get("graphsHash/actorTv.txt"),charset);
			writeDb(bdbActorTv,writer);
			bdbActorTv.close();
			writer.close();
			writer=Files.newBufferedWriter(Paths.get("graphsHash/actorEpisode.txt"),charset);
			writeDb(bdbActorEpisode,writer);
			bdbActorEpisode.close();
			writer.close();
			
			writer=Files.newBufferedWriter(Paths.get("graphsHash/episodeActor.txt"),charset);
			writeDb(bdbEpisodeActor,writer);
			bdbEpisodeActor.close();
			writer.close();
			writer=Files.newBufferedWriter(Paths.get("graphsHash/tvActor.txt"),charset);
			writeDb(bdbTvActor,writer);
			bdbTvActor.close();
			writer.close();
			
		}
		
		catch (IOException e)
		{	System.err.println(e);
			e.printStackTrace();
		}		  
		catch(DatabaseException dbe)
		{	System.err.println(dbe);
			dbe.printStackTrace();
		}	
	}
	
	public void readActorsMovie(String sourceFile)//,String destFile) //SourceFile to read the contents from
	{
		Path pathSourceFile=Paths.get(sourceFile);

		Pattern patternYear = Pattern.compile("[(][0-9?]{4}");//for matching 1st year
		Matcher matcherYear ;
		Pattern patternTV= Pattern.compile("[\"].*[\"]"); // for matching episode
		Matcher matcherTV;
		Pattern patternMovie=Pattern.compile("[\t].*[(]");
		Matcher matcherMovie; 
		String stringActorId="",stringActor="",stringPersonId="";
		String stringCharacter="";
		Pattern patternCharacter=Pattern.compile("\\[.*\\]");
		Matcher matcherCharacter;
			
		String stringPrevActor="", stringPrevTv="";
		String []lines;
		stringFile=mapFiles.get("actor");
		String stringYear="";
		try
		{
			
			bdbActorMovie=new Database("graphsHash/actorMovie.db",null,dbConfig);
			bdbMovieActor=new Database("graphsHash/movieActor.db",null,dbConfig);
		}
		catch(Exception dbe)
		{	System.err.println(dbe);
			dbe.printStackTrace();
		}
		
		
		try
		{	reader=Files.newBufferedReader(pathSourceFile,charset);
			int matchPositionYear=0,matchPositionEpisode=0; 
			long rowsMovies=0, rowsTotal=0,rowsAuthor=0,rowsTv=0;
			while ((line = reader.readLine()) != null)
			{	rowsTotal++;
				if (!(line.trim().isEmpty()))
				{	lines=line.split("\t");
					if (!(lines[0].trim().isEmpty()))
					{ 	stringActor=lines[0].trim();
						stringActorId=checkActor(stringActor,true);
					}
					try
					{	matcherYear=patternYear.matcher(line);
						if (matcherYear.find())
						{	matchPositionYear=matcherYear.start();
							if (matchPositionYear>=lines[0].length())
							{	stringYear=matcherYear.group();
								stringMovie=(line.substring(lines[0].length(),matchPositionYear)).trim();//matcherYear.start())).trim();
								if (stringMovie.charAt(0)!='\"')  //its a movie since " " missing
								{	//rowsMovies++;
									try
									{	matcherCharacter=patternCharacter.matcher(line);
										if (matcherCharacter.find())
											stringCharacter=(matcherCharacter.group().replaceAll("\\[","")).replaceAll("\\]","");
										else
											stringCharacter="";
									}
									catch(IllegalStateException ise) { ise.printStackTrace();}
									
									stringMovieId=checkMovie(stringMovie,true);
									updatebdbMovieActor(stringMovie,stringMovieId,"\t"+stringYear+"\t"+stringActor+"\t"+stringActorId+"\t"+stringCharacter);
									updatebdbActorMovie(stringActor,stringActorId,"\t"+stringMovie+"\t"+stringMovieId+"\t"+stringYear+"\t"+stringCharacter);
									
								}
							}
						}
					}
					catch(IllegalStateException ise)
					{	System.out.println("illegal state exception"+line);
					}
				}
			}
			System.out.println("Finished actors.list");
			reader.close();
			writer=Files.newBufferedWriter(Paths.get("graphsHash/actorId.txt"),charset);
			writeDb(bdbActorId,writer);
			bdbActorId.close();
			writer.close();
			writer=Files.newBufferedWriter(Paths.get("graphsHash/actorMovie.txt"),charset);
			writeDb(bdbActorMovie,writer);
			bdbActorMovie.close();
			writer.close();
			
			writer=Files.newBufferedWriter(Paths.get("graphsHash/movieActor.txt"),charset);
			writeDb(bdbMovieActor,writer);
			bdbMovieActor.close();
			writer.close();
			
		}
		
		catch (IOException e)
		{	System.err.println(e);
			e.printStackTrace();
		}		  
		catch(DatabaseException dbe)
		{	System.err.println(dbe);
			dbe.printStackTrace();
		}	
	}
	
	public void updatebdbTvActress(String stringTv, String stringTvId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue;
		byte[] byteKey,byteValue;
		byteKey=stringTv.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		byteValue=(stringTvId+stringToAdd).getBytes(charset);//(stringMovieId+"\t"+stringYear +"\t"+stringYearId).getBytes(charset);
		dbValue=new DatabaseEntry(byteValue);
			
		try
		{   bdbTvActress.put(null,dbKey,dbValue); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	}

	public void updatebdbMovieActress(String stringMovie, String stringMovieId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue;
		byte[] byteKey,byteValue, byteValueNew, byteValueOld;
		byteKey=stringMovie.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		byteValue=(stringMovieId+stringToAdd).getBytes(charset);//(stringMovieId+"\t"+stringYear +"\t"+stringYearId).getBytes(charset);
		dbValue=new DatabaseEntry(byteValue);
				
		try
		{   bdbMovieActress.put(null,dbKey,dbValue); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	}

public void updatebdbEpisodeActress(String stringEpisode, String stringEpisodeId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue;
		byte[] byteKey,byteValue, byteValueNew, byteValueOld;
		byteKey=stringEpisode.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		byteValue=(stringEpisodeId+stringToAdd).getBytes(charset);//(stringMovieId+"\t"+stringYear +"\t"+stringYearId).getBytes(charset);
		dbValue=new DatabaseEntry(byteValue);
			
		try
		{   
			bdbEpisodeActress.put(null,dbKey,dbValue); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	}

	public void updatebdbActressEpisode(String stringActress, String stringActressId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue;
		byte[] byteKey,byteValue,byteValueNew,byteValueOld;
		byteKey=stringActress.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		byteValue=(stringActressId+stringToAdd).getBytes(charset);//(stringMovieId+"\t"+stringYear +"\t"+stringYearId).getBytes(charset);
		dbValue=new DatabaseEntry(byteValue);
			
		try
		{   
		//	if (stringActress.charAt(0)<'G')
				bdbActressEpisode.put(null,dbKey,dbValue); 
		/*	else if (stringActress.charAt(0)<'L')
				bdbActressEpisode2.put(null,dbKey,dbValue); 
			else if(stringActress.charAt(0)<'P')
				bdbActressEpisode3.put(null,dbKey,dbValue); 
			else
				bdbActressEpisode4.put(null,dbKey,dbValue);
		*/}
		catch(DatabaseException dbe){System.err.println(dbe);}
	
		
	}
	public void updatebdbActressTv(String stringActress, String stringActressId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue;
		byte[] byteKey,byteValue,byteValueNew,byteValueOld;
		byteKey=stringActress.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		byteValue=(stringActressId+stringToAdd).getBytes(charset);//(stringMovieId+"\t"+stringYear +"\t"+stringYearId).getBytes(charset);
		dbValue=new DatabaseEntry(byteValue);
			
		try
		{   bdbActressTv.put(null,dbKey,dbValue); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	}

	public void updatebdbActressMovie(String stringActress, String stringActressId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue;
		byte[] byteKey,byteValue,byteValueOld,byteValueNew;
		byteKey=stringActress.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		byteValue=(stringActressId+stringToAdd).getBytes(charset);//(stringMovieId+"\t"+stringYear +"\t"+stringYearId).getBytes(charset);
		dbValue=new DatabaseEntry(byteValue);
		try
		{   bdbActressMovie.put(null,dbKey,dbValue); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	}

	public String checkActress(String stringActress,Boolean booleanAdd)
	{	DatabaseEntry dbActress, dbActressId;
		byte[] byteActress,byteActressId;
		String stringActressIMDb;
		byteActress=stringActress.getBytes(charset);
		dbActress=new DatabaseEntry(byteActress);	
		dbActressId=new DatabaseEntry();
		String stringActressId="", stringPersonValue="";
		try
		{
			if(bdbActressId.exists(null,dbActress)==OperationStatus.SUCCESS)
			{	if(bdbActressId.get(null,dbActress,dbActressId,null)==OperationStatus.SUCCESS)
				{	stringActressId=new String(dbActressId.getData(),charset);
					//return stringActorId;
				}
			}
			else if (booleanAdd)
			{	longActressId++;longPersonId++;
				stringActressId=stringFile+Long.toString(longActressId);
				byteActressId=stringActressId.getBytes(charset);//(Long.toString(yearId)).getBytes();
				dbActressId=new DatabaseEntry(byteActressId);
				bdbActressId.put(null,dbActress,dbActressId);
				stringPersonValue=stringPersonFile+Long.toString(longPersonId)+"\t"+stringActressId;
				byteActressId=stringPersonValue.getBytes(charset);
				dbActressId=new DatabaseEntry(byteActressId);
				bdbPerson.put(null,dbActress,dbActressId);			
			}
		}
		catch(DatabaseException dbe)
		{	System.err.println(dbe);
			dbe.printStackTrace();
		}
		return stringActressId;
	}
	
	public void readActressEpisode(String sourceFile)//,String destFile) //SourceFile to read the contents from
	{
		Path pathSourceFile=Paths.get(sourceFile);
		//Path pathDestFile=Paths.get(destFile);//"graphs/movieGraphIMDb.txt");

		Pattern patternYear = Pattern.compile("[(][0-9?]{4}");//for matching 1st year
		Matcher matcherYear ;
		Pattern patternTV= Pattern.compile("[\"].*[\"]"); // for matching episode
		Matcher matcherTV;
		Pattern patternEpisode=Pattern.compile("[{].*[}]");
		Matcher matcherEpisode;
		String stringActressId="",stringActress="",stringPersonId="";
		String stringPrevActress="",stringPrevTv="";
		String []lines;
		stringFile=mapFiles.get("actress");
		String stringCharacter="";
		String stringYear="";
		Pattern patternCharacter=Pattern.compile("\\[.*\\]");
		Matcher matcherCharacter;
		try
		{	bdbActressId=new Database("graphsHash/actressId.db",null,dbConfig);
			bdbActressTv=new Database("graphsHash/actressTv.db",null,dbConfig);
			bdbActressEpisode=new Database("graphsHash/actressEpisode.db",null,dbConfig);
			bdbTvActress=new Database("graphsHash/TvActress.db",null,dbConfig);
			bdbEpisodeActress=new Database("graphsHash/episodeActress.db",null,dbConfig);
			if (bdbPerson==null) bdbPerson=new Database("graphsHash/person.db",null,dbConfig);
			if (bdbPersonId==null) bdbPersonId=new Database("graphsHash/personId.db",null,dbConfig);
		
		}
		catch(Exception dbe)
		{	System.err.println(dbe);
			dbe.printStackTrace();
		}
						
		
		try
		{	reader=Files.newBufferedReader(pathSourceFile,charset);
			int matchPositionYear=0,matchPositionEpisode=0; 
			long rowsLines=0,rowsActress=0,rowsTv=0,rowsEpisode=0;
			while ((line = reader.readLine()) != null)
			{	rowsLines++;
				if (!(line.trim().isEmpty()))
				{	lines=line.split("\t");
					if (!(lines[0].trim().isEmpty()))
					{ 	stringActress=lines[0].trim();
						stringActressId=checkActress(stringActress,true);
						//stringPersonId=checkPerson(stringActress,true);
						rowsActress++;
					}
					//System.out.println("stringActor:"+stringActor+" stringActorId:" +stringActorId);
					try
					{	matcherYear=patternYear.matcher(line);
						if (matcherYear.find())
						{	matchPositionYear=matcherYear.start();
							if (matchPositionYear>=lines[0].length())
							{	stringYear=matcherYear.group();
								stringMovie=(line.substring(lines[0].length(),matchPositionYear)).trim();//matcherYear.start())).trim();
								if (stringMovie.charAt(0)=='\"')  //its a Tv series " " 
								{	try
									{	matcherCharacter=patternCharacter.matcher(line);
										if (matcherCharacter.find())
											stringCharacter=(matcherCharacter.group().replaceAll("\\[","")).replaceAll("\\]","");
										else
											stringCharacter="";
									}
									catch(IllegalStateException ise) { ise.printStackTrace();}
									
									matcherEpisode=patternEpisode.matcher(line);
									try
									{	if (matcherEpisode.find())
										{	stringEpisode=((matcherEpisode.group()).replaceAll("\\{","")).replaceAll("\\}","");
											
											stringEpisodeId=checkEpisode(stringEpisode,true);
											updatebdbEpisodeActress(stringEpisode,stringEpisodeId,"\t"+stringActress+"\t"+stringActressId);
											updatebdbActressEpisode(stringActress,stringActressId,"\t"+stringEpisode+"\t"+stringEpisodeId);
												
										}
									}
									catch(IllegalStateException ise)
									{	System.err.println(ise);
										ise.printStackTrace();
									}
									//if (stringTvId!="" && !(stringPrevTv.equals(stringMovie)))
									if (!(stringPrevTv.equals(stringMovie)))
									{
										stringTvId=checkTv(stringMovie,true);
										updatebdbTvActress(stringMovie,stringTvId,"\t"+stringYear+"\t"+stringActress+"\t"+stringActressId+ "\t"+stringCharacter);
										updatebdbActressTv(stringActress,stringActressId,"\t"+stringMovie+"\t"+stringTvId+ "\t"+stringYear+"\t"+stringCharacter);
								
									}
								
								}
							}
						}
					}
					catch(IllegalStateException ise)
					{	System.out.println("illegal state exception"+line);
					}
				}
				//stringPrevActor=stringActor;
			}
			reader.close();

			writer=Files.newBufferedWriter(Paths.get("graphsHash/actressTv.txt"),charset);
			writeDb(bdbActressTv,writer);
			bdbActressTv.close();
			writer.close();	
			writer=Files.newBufferedWriter(Paths.get("graphsHash/actressEpisode.txt"),charset);
			writeDb(bdbActressEpisode,writer);
			bdbActressEpisode.close();
			writer.close();	
			
			writer=Files.newBufferedWriter(Paths.get("graphsHash/tvActress.txt"),charset);
			writeDb(bdbTvActress,writer);
			bdbTvActress.close();
			writer.close();	
			writer=Files.newBufferedWriter(Paths.get("graphsHash/episodeActress.txt"),charset);
			writeDb(bdbEpisodeActress,writer);
			bdbEpisodeActress.close();
			writer.close();	
		}
		
		catch (IOException e)
		{	System.err.println(e);
			e.printStackTrace();
		}
		catch(DatabaseException dbe)
		{	System.err.println(dbe);
			dbe.printStackTrace();
		}		  
		
		
	}
	
	public void readActressMovie(String sourceFile)//,String destFile) //SourceFile to read the contents from
	{
		Path pathSourceFile=Paths.get(sourceFile);
		//Path pathDestFile=Paths.get(destFile);//"graphs/movieGraphIMDb.txt");

		Pattern patternYear = Pattern.compile("[(][0-9?]{4}");//for matching 1st year
		Matcher matcherYear ;
		Pattern patternMovie=Pattern.compile("[\t].*[(]");
		Matcher matcherMovie; 
		String stringActressId="",stringActress="",stringPersonId="";
		String stringPrevActress="",stringPrevTv="";
		String []lines;
		stringFile=mapFiles.get("actress");
		String stringCharacter="";
		String stringYear;
		Pattern patternCharacter=Pattern.compile("\\[.*\\]");
		Matcher matcherCharacter;
		
		try
		{	bdbActressId=new Database("graphsHash/actressId.db",null,dbConfig);
			bdbActressMovie=new Database("graphsHash/actressMovie.db",null,dbConfig);
			bdbMovieActress=new Database("graphsHash/movieActress.db",null,dbConfig);
		}
		catch(Exception dbe)
		{	System.err.println(dbe);
			dbe.printStackTrace();
		}
						
		
		try
		{	reader=Files.newBufferedReader(pathSourceFile,charset);
			//writer=Files.newBufferedWriter(pathDestFile,charset);
			int matchPositionYear=0,matchPositionEpisode=0; 
			long rowsMovies=0, rowsLines=0,rowsActress=0,rowsTv=0,rowsEpisode=0;
			while ((line = reader.readLine()) != null)
			{	rowsLines++;
				if (!(line.trim().isEmpty()))
				{	lines=line.split("\t");
					if (!(lines[0].trim().isEmpty()))
					{ 	stringActress=lines[0].trim();
						stringActressId=checkActress(stringActress,true);
						//stringPersonId=checkPerson(stringActress,true);
						rowsActress++;
					}
					//System.out.println("stringActor:"+stringActor+" stringActorId:" +stringActorId);
					try
					{	matcherYear=patternYear.matcher(line);
						if (matcherYear.find())
						{	matchPositionYear=matcherYear.start();
							if (matchPositionYear>=lines[0].length())
							{	stringYear=matcherYear.group();
								stringMovie=(line.substring(lines[0].length(),matchPositionYear)).trim();//matcherYear.start())).trim();
								if (stringMovie.charAt(0)!='\"')  //its a movie since " " missing
								{	rowsMovies++;
									try
									{	matcherCharacter=patternCharacter.matcher(line);
										if (matcherCharacter.find())
											stringCharacter=(matcherCharacter.group().replaceAll("\\[","")).replaceAll("\\]","");
										else
											stringCharacter="";
									}
									catch(IllegalStateException ise) { ise.printStackTrace();}
									
									stringMovieId=checkMovie(stringMovie,true);
									updatebdbMovieActress(stringMovie,stringMovieId,"\t"+stringYear+"\t"+stringActress+"\t"+stringActressId + "\t"+stringCharacter);
									updatebdbActressMovie(stringActress,stringActressId,"\t"+stringMovie+"\t"+stringTvId+ "\t"+stringYear+"\t"+stringCharacter);
								}
								
								
							}
						}
					}
					catch(IllegalStateException ise)
					{	System.out.println("illegal state exception"+line);
					}
				}
				//stringPrevActor=stringActor;
			}
			reader.close();

			writer=Files.newBufferedWriter(Paths.get("graphsHash/actressId.txt"),charset);
			writeDb(bdbActressId,writer);
			bdbActressId.close();
			writer.close();
			writer=Files.newBufferedWriter(Paths.get("graphsHash/actressMovie.txt"),charset);
			writeDb(bdbActressMovie,writer);
			bdbActressMovie.close();
			writer.close();
			writer=Files.newBufferedWriter(Paths.get("graphsHash/movieActress.txt"),charset);
			writeDb(bdbMovieActress,writer);
			bdbMovieActress.close();
			writer.close();
		}
		
		catch (IOException e)
		{	System.err.println(e);
			e.printStackTrace();
		}
		catch(DatabaseException dbe)
		{	System.err.println(dbe);
			dbe.printStackTrace();
		}		  
		
		
	}

	public void updatebdbMovieDirector(String stringMovie, String stringMovieId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue;
		byte[] byteKey,byteValue, byteValueNew, byteValueOld;
		byteKey=stringMovie.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		byteValue=(stringMovieId+stringToAdd).getBytes(charset);//(stringMovieId+"\t"+stringYear +"\t"+stringYearId).getBytes(charset);
		dbValue=new DatabaseEntry(byteValue);
			
		try
		{   bdbMovieDirector.put(null,dbKey,dbValue); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	
	}
	public void updatebdbEpisodeDirector(String stringEpisode, String stringEpisodeId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue;
		byte[] byteKey,byteValue, byteValueOld, byteValueNew;
		byteKey=stringEpisode.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		byteValue=(stringEpisodeId+stringToAdd).getBytes(charset);//(stringMovieId+"\t"+stringYear +"\t"+stringYearId).getBytes(charset);
		dbValue=new DatabaseEntry(byteValue);
			
		try
		{   bdbEpisodeDirector.put(null,dbKey,dbValue); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	
	}

	public void updatebdbTvDirector(String stringTv, String stringTvId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue;
		byte[] byteKey,byteValue, byteValueOld,byteValueNew;
		byteKey=stringTv.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		byteValue=(stringTvId+stringToAdd).getBytes(charset);//(stringMovieId+"\t"+stringYear +"\t"+stringYearId).getBytes(charset);
		dbValue=new DatabaseEntry(byteValue);
			
		try
		{   bdbTvDirector.put(null,dbKey,dbValue); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	
	}

	
	public void updatebdbDirectorMovie(String stringDirector, String stringDirectorId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue;
		byte[] byteKey,byteValue, byteValueNew, byteValueOld;
		byteKey=stringDirector.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		byteValue=(stringDirectorId+stringToAdd).getBytes(charset);//(stringMovieId+"\t"+stringYear +"\t"+stringYearId).getBytes(charset);
		dbValue=new DatabaseEntry(byteValue);
			
		try
		{   bdbDirectorMovie.put(null,dbKey,dbValue); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	
	}

	public void updatebdbDirectorEpisode(String stringDirector, String stringDirectorId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue;
		byte[] byteKey,byteValue, byteValueOld, byteValueNew;
		byteKey=stringDirector.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		byteValue=(stringDirectorId+stringToAdd).getBytes(charset);//(stringMovieId+"\t"+stringYear +"\t"+stringYearId).getBytes(charset);
		dbValue=new DatabaseEntry(byteValue);
			
		try
		{   bdbDirectorEpisode.put(null,dbKey,dbValue); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	
	}
	
public void updatebdbDirectorTv(String stringDirector, String stringDirectorId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue;
		byte[] byteKey,byteValue, byteValueOld,byteValueNew;
		byteKey=stringDirector.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		byteValue=(stringDirectorId+stringToAdd).getBytes(charset);//(stringMovieId+"\t"+stringYear +"\t"+stringYearId).getBytes(charset);
		dbValue=new DatabaseEntry(byteValue);
			
		try
		{   bdbDirectorTv.put(null,dbKey,dbValue); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	
		
	}

	public String checkDirector(String stringDirector,Boolean booleanAdd)
	{	DatabaseEntry dbDirector, dbDirectorId;
		byte[] byteDirector,byteDirectorId;
		String stringDirectorIMDb;
		byteDirector=stringDirector.getBytes(charset);
		dbDirector=new DatabaseEntry(byteDirector);	
		dbDirectorId=new DatabaseEntry();
		String stringDirectorId="";
		try
		{
			if(bdbDirectorId.exists(null,dbDirector)==OperationStatus.SUCCESS)
			{	if(bdbDirectorId.get(null,dbDirector,dbDirectorId,null)==OperationStatus.SUCCESS)
				{	stringDirectorId=new String(dbDirectorId.getData(),charset);
					//return stringActorId;
				}
			}
			else if (booleanAdd)
			{	longDirectorId++;
				stringDirectorId=stringFile+Long.toString(longDirectorId);
				byteDirectorId=stringDirectorId.getBytes(charset);//(Long.toString(yearId)).getBytes();
				dbDirectorId=new DatabaseEntry(byteDirectorId);
				bdbDirectorId.put(null,dbDirector,dbDirectorId);
				updatebdbPerson(stringDirector,stringDirectorId);
			}
		}
		catch(DatabaseException dbe)
		{	System.err.println(dbe);
			dbe.printStackTrace();
		}
		return stringDirectorId;
	}
	
	public void readDirector(String sourceFile)//,String destFile) //SourceFile to read the contents from
	{
		Path pathSourceFile=Paths.get(sourceFile);
		//Path pathDestFile=Paths.get(destFile);//"graphs/movieGraphIMDb.txt");

		Pattern patternYear = Pattern.compile("[(][0-9?]{4}");//for matching 1st year
		Matcher matcherYear ;
		Pattern patternTV= Pattern.compile("[\"].*[\"]"); // for matching episode
		Matcher matcherTV;
		Pattern patternMovie=Pattern.compile("[\t].*[(]");
		Matcher matcherMovie; 
		Pattern patternEpisode=Pattern.compile("[{].*[}]");
		Matcher matcherEpisode;
		String stringDirectorId="",stringDirector="", stringPersonId="";
		String stringPrevDirector="",stringPrevTv="";
		String []lines;
		String stringYear;
		stringFile=mapFiles.get("director");
		try
		{
			bdbDirectorId=new Database("graphsHash/directorId.db",null,dbConfig);
			bdbDirectorMovie=new Database("graphsHash/directorMovie.db",null,dbConfig);
			bdbDirectorEpisode=new Database("graphsHash/directorEpisode.db",null,dbConfig);
			bdbDirectorTv=new Database("graphsHash/directorTv.db",null,dbConfig);
			bdbMovieDirector=new Database("graphsHash/movieDirector.db",null,dbConfig);
			bdbTvDirector=new Database("graphsHash/tvDirector.db",null,dbConfig);
			bdbEpisodeDirector=new Database("graphsHash/episodeDirector.db",null,dbConfig);
			if (bdbPerson==null) bdbPerson=new Database("graphsHash/person.db",null,dbConfig);
			if (bdbPersonId==null) bdbPersonId=new Database("graphsHash/personId.db",null,dbConfig);
			
		}
		catch(Exception dbe)
		{	System.err.println(dbe);
			dbe.printStackTrace();
		}
		
		try
		{	reader=Files.newBufferedReader(pathSourceFile,charset);
			//writer=Files.newBufferedWriter(pathDestFile,charset);
			int matchPositionYear=0,matchPositionEpisode=0; 
			long rowsMovies=0, rowsLines=0,rowsDirector=0,rowsTv=0,rowsEpisode=0;
			while ((line = reader.readLine()) != null)
			{	rowsLines++;
				if (!(line.trim().isEmpty()))
				{//	System.out.println("line:"+line);
					lines=line.split("\t");
					if (!(lines[0].trim().isEmpty()))
					{ 	stringDirector=lines[0].trim();
						stringDirectorId=checkDirector(stringDirector,true);
						//stringPersonId=checkPerson(stringDirector,true);
						rowsDirector++;
					}
					//System.out.println("stringActor:"+stringActor+" stringActorId:" +stringActorId);
					try
					{	matcherYear=patternYear.matcher(line);
						if (matcherYear.find())
						{	matchPositionYear=matcherYear.start();
							if (matchPositionYear>=lines[0].length())
							{	stringYear=matcherYear.group();
								stringMovie=(line.substring(lines[0].length(),matchPositionYear)).trim();//matcherYear.start())).trim();
								if (stringMovie.charAt(0)!='\"')  //its a movie since " " missing
								{	rowsMovies++;
									stringMovieId=checkMovie(stringMovie,true);
									updatebdbMovieDirector(stringMovie,stringMovieId,"\t"+stringYear+"\t"+stringDirector+"\t"+stringDirectorId);
									updatebdbDirectorMovie(stringDirector,stringDirectorId,"\t"+stringMovie+"\t"+stringMovieId+"\t"+stringYear);
								
									}
								else  //its a TV series
								{	
									matcherEpisode=patternEpisode.matcher(line);
									try
									{	if (matcherEpisode.find())
										{	stringEpisode=((matcherEpisode.group()).replaceAll("\\{","")).replaceAll("\\}","");
											
											stringEpisodeId=checkEpisode(stringEpisode,true);
											updatebdbEpisodeDirector(stringEpisode,stringEpisodeId,"\t"+stringDirector+"\t"+stringDirectorId);
											updatebdbDirectorEpisode(stringDirector,stringDirectorId,"\t"+stringEpisode+"\t"+stringEpisodeId);
										
										}
									}
									catch(IllegalStateException ise)
									{	System.err.println(ise);
										ise.printStackTrace();
									}
									//if (stringTvId!="" && !(stringPrevTv.equals(stringMovie)))
									if (!(stringPrevTv.equals(stringMovie)))
									{
										stringTvId=checkTv(stringMovie,true);
										rowsTv++;
										updatebdbTvDirector(stringMovie,stringTvId,"\t"+stringYear+"\t"+stringDirector+"\t"+stringDirectorId);
										updatebdbDirectorTv(stringDirector,stringDirectorId,"\t"+stringMovie+ "\t"+stringTvId+"\t"+stringYear);
									
									}
								
								}
							}
						}
					}
					catch(IllegalStateException ise)
					{	System.out.println("illegal state exception"+line);
					}
				}
				//stringPrevActor=stringActor;
			}
			reader.close();

			writer=Files.newBufferedWriter(Paths.get("graphsHash/directorId.txt"),charset);
			writeDb(bdbDirectorId,writer);
			bdbDirectorId.close();
			writer.close();
			writer=Files.newBufferedWriter(Paths.get("graphsHash/directorMovie.txt"),charset);
			writeDb(bdbDirectorMovie,writer);
			bdbDirectorMovie.close();
			writer.close();			
			writer=Files.newBufferedWriter(Paths.get("graphsHash/directorEpisode.txt"),charset);
			writeDb(bdbDirectorEpisode,writer);
			bdbDirectorEpisode.close();
			writer.close();			
			writer=Files.newBufferedWriter(Paths.get("graphsHash/directorTv.txt"),charset);
			writeDb(bdbDirectorTv,writer);
			bdbDirectorTv.close();
			writer.close();			
		
			writer=Files.newBufferedWriter(Paths.get("graphsHash/movieDirector.txt"),charset);
			writeDb(bdbMovieDirector,writer);
			bdbMovieDirector.close();
			writer.close();			
			writer=Files.newBufferedWriter(Paths.get("graphsHash/tvDirector.txt"),charset);
			writeDb(bdbTvDirector,writer);
			bdbTvDirector.close();
			writer.close();			
			writer=Files.newBufferedWriter(Paths.get("graphsHash/episodeDirector.txt"),charset);
			writeDb(bdbEpisodeDirector,writer);
			bdbEpisodeDirector.close();
			writer.close();			
		}
		
		catch (IOException e)
		{	System.err.println(e);
			e.printStackTrace();
		}		  
		catch(DatabaseException dbe)
		{	System.err.println(dbe);
			dbe.printStackTrace();
		}
		
	}


	public void updatebdbTvProducer(String stringTv, String stringTvId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue;
		byte[] byteKey,byteValue, byteValueNew, byteValueOld;
		byteKey=stringTv.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		byteValue=(stringTvId+stringToAdd).getBytes(charset);//(stringMovieId+"\t"+stringYear +"\t"+stringYearId).getBytes(charset);
		dbValue=new DatabaseEntry(byteValue);
			
	
		try
		{   bdbTvProducer.put(null,dbKey,dbValue); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
		
		
	}
	public void updatebdbEpisodeProducer(String stringEpisode, String stringEpisodeId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue;
		byte[] byteKey,byteValue, byteValueNew, byteValueOld;
		byteKey=stringEpisode.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		byteValue=(stringEpisodeId+stringToAdd).getBytes(charset);//(stringMovieId+"\t"+stringYear +"\t"+stringYearId).getBytes(charset);
		dbValue=new DatabaseEntry(byteValue);
		try
		{   bdbEpisodeProducer.put(null,dbKey,dbValue); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
		
	}

	public void updatebdbMovieProducer(String stringMovie, String stringMovieId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue;
		byte[] byteKey,byteValue, byteValueNew, byteValueOld;
		byteKey=stringMovie.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		byteValue=(stringMovieId+stringToAdd).getBytes(charset);//(stringMovieId+"\t"+stringYear +"\t"+stringYearId).getBytes(charset);
		dbValue=new DatabaseEntry(byteValue);
			

		try
		{   bdbMovieProducer.put(null,dbKey,dbValue); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
		
	}


	public void updatebdbProducerMovie(String stringProducer, String stringProducerId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue;
		byte[] byteKey,byteValue, byteValueNew, byteValueOld;
		byteKey=stringProducer.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		byteValue=(stringProducerId+stringToAdd).getBytes(charset);//(stringMovieId+"\t"+stringYear +"\t"+stringYearId).getBytes(charset);
		dbValue=new DatabaseEntry(byteValue);
		try
		{   bdbProducerMovie.put(null,dbKey,dbValue); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	}

	public void updatebdbProducerTv(String stringProducer, String stringProducerId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue;
		byte[] byteKey,byteValue,byteValueNew, byteValueOld;
		byteKey=stringProducer.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		byteValue=(stringProducerId+stringToAdd).getBytes(charset);//(stringMovieId+"\t"+stringYear +"\t"+stringYearId).getBytes(charset);
		dbValue=new DatabaseEntry(byteValue);
		try
		{   bdbProducerTv.put(null,dbKey,dbValue); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	}

	public void updatebdbProducerEpisode(String stringProducer, String stringProducerId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue;
		byte[] byteKey,byteValue,byteValueNew,byteValueOld;
		byteKey=stringProducer.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		byteValue=(stringProducerId+stringToAdd).getBytes(charset);//(stringMovieId+"\t"+stringYear +"\t"+stringYearId).getBytes(charset);
		dbValue=new DatabaseEntry(byteValue);
		try
		{   bdbProducerEpisode.put(null,dbKey,dbValue); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
			
	}

	
	public String checkProducer(String stringProducer,Boolean booleanAdd)
	{	DatabaseEntry dbProducer, dbProducerId;
		byte[] byteProducer,byteProducerId;
		String stringProducerIMDb;
		byteProducer=stringProducer.getBytes(charset);
		dbProducer=new DatabaseEntry(byteProducer);	
		dbProducerId=new DatabaseEntry();
		String stringProducerId="";
		try
		{
			if(bdbProducerId.exists(null,dbProducer)==OperationStatus.SUCCESS)
			{	if(bdbProducerId.get(null,dbProducer,dbProducerId,null)==OperationStatus.SUCCESS)
				{	stringProducerId=new String(dbProducerId.getData(),charset);
					//return stringActorId;
				}
			}
			else if (booleanAdd)
			{	longProducerId++;
				stringProducerId=stringFile+Long.toString(longProducerId);
				byteProducerId=stringProducerId.getBytes(charset);//(Long.toString(yearId)).getBytes();
				dbProducerId=new DatabaseEntry(byteProducerId);
				bdbProducerId.put(null,dbProducer,dbProducerId);
				updatebdbPerson(stringProducer, stringProducerId);
			}
		}
		catch(DatabaseException dbe)
		{	System.err.println(dbe);
			dbe.printStackTrace();
		}
		return stringProducerId;
	}
	
	public void readProducer(String sourceFile)//,String destFile) //SourceFile to read the contents from
	{
		Path pathSourceFile=Paths.get(sourceFile);

		Pattern patternYear = Pattern.compile("[(][0-9?]{4}");//for matching 1st year
		Matcher matcherYear ;
		Pattern patternTV= Pattern.compile("[\"].*[\"]"); // for matching episode
		Matcher matcherTV;
//		Pattern patternMovie=Pattern.compile("[\t].*[(]");
//		Matcher matcherMovie; 
		Pattern patternProducer=Pattern.compile("[ (][a-zA-Z- ]*producer[a-zA-Z- ]*[)]");
		Matcher matcherProducer;
		Pattern patternEpisode=Pattern.compile("[{].*[}]");
		Matcher matcherEpisode;
		String stringProducerId="",stringProducer="";
		String stringPrevProducer="",stringPrevTv="",stringProducerType="", stringPersonId="";
		String []lines;
		String stringYear;
		stringFile=mapFiles.get("producer");
		try
		{
			bdbProducerMovie=new Database("graphsHash/producerMovie.db",null,dbConfig);
			bdbProducerEpisode=	new Database("graphsHash/producerEpisode.db",null,dbConfig);
			bdbProducerTv=new Database("graphsHash/producerTv.db",null,dbConfig);
			bdbMovieProducer=new Database("graphsHash/movieProducer.db",null,dbConfig);
			bdbTvProducer=new Database("graphsHash/tvProducer.db",null,dbConfig);
			bdbEpisodeProducer=new Database("graphsHash/episodeProducer.db",null,dbConfig);
			bdbProducerId=new Database("graphsHash/producerId.db",null,dbConfig);
			if (bdbPerson ==null)	bdbPerson=new Database("graphsHash/person.db",null,dbConfig);
			if (bdbPersonId==null)  bdbPersonId=new Database("graphsHash/personId.db",null,dbConfig);

		}
		catch(Exception dbe)
		{	System.err.println(dbe);
			dbe.printStackTrace();
		}
		
		
		try
		{	reader=Files.newBufferedReader(pathSourceFile,charset);
			int matchPositionYear=0,matchPositionEpisode=0; 
			long rowsMovies=0, rowsLines=0,rowsProducer=0,rowsTv=0,rowsEpisode=0;
			while ((line = reader.readLine()) != null)
			{	rowsLines++;
				if (!(line.trim().isEmpty()))
				{	lines=line.split("\t");
					if (!(lines[0].trim().isEmpty()))
					{ 	stringProducer=lines[0].trim();
						stringProducerId=checkProducer(stringProducer,true);
						//stringPersonId=checkPerson(stringProducer,true);
						rowsProducer++;
					}
					try
					{	matcherYear=patternYear.matcher(line);
						if (matcherYear.find())
						{	matchPositionYear=matcherYear.start();
							if (matchPositionYear>=lines[0].length())
							{	stringYear=matcherYear.group();
								stringMovie=(line.substring(lines[0].length(),matchPositionYear)).trim();//matcherYear.start())).trim();
								matcherProducer=patternProducer.matcher(line);
								if (matcherProducer.find(matcherYear.end()))
								{	stringProducerType=(matcherProducer.group()).trim();
									//stringProducerType=(stringProducerType.substring(1,stringProducerType.length()-1));
								}
								else	
									stringProducerType="";
								if (stringMovie.charAt(0)!='\"')  //its a movie since " " missing
								{	rowsMovies++;
									stringMovieId=checkMovie(stringMovie,true);
									updatebdbMovieProducer(stringMovie,stringMovieId,"\t"+stringYear+"\t"+stringProducer+"\t"+stringProducerId+"\t" +stringProducerType);
									updatebdbProducerMovie(stringProducer,stringProducerId,"\t"+stringMovie+"\t"+stringMovieId+"\t"+stringYear+"\t"+stringProducerType);
								
								}
								else  //its a TV series
								{	
									matcherEpisode=patternEpisode.matcher(line);
									try
									{	if (matcherEpisode.find())
										{	stringEpisode=((matcherEpisode.group()).replaceAll("\\{","")).replaceAll("\\}","");
											
											stringEpisodeId=checkEpisode(stringEpisode,true);
											//if (stringEpisodeId!="")
											if (matcherProducer.find(matcherEpisode.end()))
											{	stringProducerType=matcherProducer.group();
												stringProducerType=(stringProducerType.substring(1,stringProducerType.length()-1));
												updatebdbEpisodeProducer(stringEpisode,stringEpisodeId,"\t"+stringProducer+"\t"+stringProducerId+"\t" +stringProducerType);
												updatebdbProducerEpisode(stringProducer,stringProducerId,"\t"+stringEpisode+"\t"+stringEpisodeId+"\t"+stringProducerType);
												
											}
									
										}
									}
									catch(IllegalStateException ise)
									{	System.err.println(ise);
										ise.printStackTrace();
									}
									//if (stringTvId!="" && !(stringPrevTv.equals(stringMovie)))
									if (!(stringPrevTv.equals(stringMovie)))
									{
										stringTvId=checkTv(stringMovie,true);
										updatebdbTvProducer(stringMovie,stringTvId,"\t"+stringYear+"\t"+stringProducer+"\t"+stringProducerId+"\t" +stringProducerType);
										updatebdbProducerTv(stringProducer,stringProducerId,"\t"+stringMovie+"\t"+stringTvId+"\t"+stringYear+"\t"+stringProducerType);
									
									}
								
								}
							}
						}
					}
					catch(IllegalStateException ise)
					{	System.out.println("illegal state exception"+line);
					}
				}
				//stringPrevActor=stringActor;
			}
			reader.close();

			writer=Files.newBufferedWriter(Paths.get("graphsHash/producerId.txt"),charset);
			writeDb(bdbProducerId,writer);
			bdbProducerId.close();
			writer.close();
			writer=Files.newBufferedWriter(Paths.get("graphsHash/producerMovie.txt"),charset);
			writeDb(bdbProducerMovie,writer);
			bdbProducerMovie.close();
			writer.close();			
			writer=Files.newBufferedWriter(Paths.get("graphsHash/producerTv.txt"),charset);
			writeDb(bdbProducerTv,writer);
			bdbProducerTv.close();
			writer.close();			
			writer=Files.newBufferedWriter(Paths.get("graphsHash/producerEpisode.txt"),charset);
			writeDb(bdbProducerEpisode,writer);
			bdbProducerEpisode.close();
			writer.close();			
		
			writer=Files.newBufferedWriter(Paths.get("graphsHash/movieProducer.txt"),charset);
			writeDb(bdbMovieProducer,writer);
			bdbMovieProducer.close();
			writer.close();			
			writer=Files.newBufferedWriter(Paths.get("graphsHash/tvProducer.txt"),charset);
			writeDb(bdbTvProducer,writer);
			bdbTvProducer.close();
			writer.close();			
			writer=Files.newBufferedWriter(Paths.get("graphsHash/episodeProducer.txt"),charset);
			writeDb(bdbEpisodeProducer,writer);
			bdbEpisodeProducer.close();
			writer.close();			
		}
		
		catch (IOException e)
		{	System.err.println(e);
			e.printStackTrace();
		}		  
		catch(DatabaseException dbe)
		{	System.err.println(dbe);
			dbe.printStackTrace();
		}
		
	}
	
	public void updatebdbTvCountry(String stringTv, String stringTvId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue;
		byte[] byteKey,byteValue;
		byteKey=stringTv.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		byteValue=(stringTvId+stringToAdd).getBytes(charset);//(stringMovieId+"\t"+stringYear +"\t"+stringYearId).getBytes(charset);
		dbValue=new DatabaseEntry(byteValue);
			
		try
		{ //comment following if, cause to check for key,value duplicates, cursor has to be opened. since we cannot 
			//if (bdbTvCountry.get(null,dbKey,dbValue,null)!=OperationStatus.SUCCESS)
			bdbTvCountry.put(null,dbKey,dbValue); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	}

	public void updatebdbEpisodeCountry(String stringEpisode, String stringEpisodeId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue;
		byte[] byteKey,byteValue;
		byteKey=stringEpisode.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		byteValue=(stringEpisodeId+stringToAdd).getBytes(charset);//(stringMovieId+"\t"+stringYear +"\t"+stringYearId).getBytes(charset);
		dbValue=new DatabaseEntry(byteValue);
			
		try
		{ //  if (bdbEpisodeCountry.get(null,dbKey,dbValue,null)!=OperationStatus.SUCCESS)
			bdbEpisodeCountry.put(null,dbKey,dbValue); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	}

	public void updatebdbMovieCountry(String stringMovie, String stringMovieId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue;
		byte[] byteKey,byteValue;
		byteKey=stringMovie.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		byteValue=(stringMovieId+stringToAdd).getBytes(charset);//(stringMovieId+"\t"+stringYear +"\t"+stringYearId).getBytes(charset);
		dbValue=new DatabaseEntry(byteValue);
			
		try
		{ //  if (bdbMovieCountry.get(null,dbKey,dbValue,null)!=OperationStatus.SUCCESS)
			bdbMovieCountry.put(null,dbKey,dbValue); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	}

	public void updatebdbCountryMovie(String stringCountry, String stringCountryId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue;
		byte[] byteKey,byteValue;
		byteKey=stringCountry.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		byteValue=(stringCountryId+stringToAdd).getBytes(charset);//(stringMovieId+"\t"+stringYear +"\t"+stringYearId).getBytes(charset);
		dbValue=new DatabaseEntry(byteValue);
			
		try
		{//   if (bdbCountryMovie.get(null,dbKey,dbValue,null)!=OperationStatus.SUCCESS)
			bdbCountryMovie.put(null,dbKey,dbValue); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	}
	public void updatebdbCountryTv(String stringCountry, String stringCountryId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue;
		byte[] byteKey,byteValue;
		byteKey=stringCountry.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		byteValue=(stringCountryId+stringToAdd).getBytes(charset);//(stringMovieId+"\t"+stringYear +"\t"+stringYearId).getBytes(charset);
		dbValue=new DatabaseEntry(byteValue);
			
		try
		{//   if (bdbCountryTv.get(null,dbKey,dbValue,null)!=OperationStatus.SUCCESS)
				bdbCountryTv.put(null,dbKey,dbValue); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	}

	public void updatebdbCountryEpisode(String stringCountry, String stringCountryId, String stringToAdd)
	{	DatabaseEntry dbKey, dbValue;
		byte[] byteKey,byteValue;
		byteKey=stringCountry.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		byteValue=(stringCountryId+stringToAdd).getBytes(charset);//(stringMovieId+"\t"+stringYear +"\t"+stringYearId).getBytes(charset);
		dbValue=new DatabaseEntry(byteValue);
			
		try
		{//   if ( bdbCountryEpisode.get(null,dbKey,dbValue,null)!=OperationStatus.SUCCESS)
			bdbCountryEpisode.put(null,dbKey,dbValue); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	}

	public String checkCountry(String stringCountry,Boolean booleanAdd)
	{	DatabaseEntry dbCountry, dbCountryId;
		byte[] byteCountry,byteCountryId;
		String stringCountryIMDb;
		byteCountry=stringCountry.getBytes(charset);
		dbCountry=new DatabaseEntry(byteCountry);	
		dbCountryId=new DatabaseEntry();
		String stringCountryId="";
		try
		{
			if(bdbCountryId.exists(null,dbCountry)==OperationStatus.SUCCESS)
			{	if(bdbCountryId.get(null,dbCountry,dbCountryId,null)==OperationStatus.SUCCESS)
				{	stringCountryId=new String(dbCountryId.getData(),charset);
					//return stringActorId;
				}
			}
			else if (booleanAdd)
			{	longCountryId++;
				stringCountryId=stringFile+Long.toString(longCountryId);
				byteCountryId=stringCountryId.getBytes(charset);//(Long.toString(yearId)).getBytes();
				dbCountryId=new DatabaseEntry(byteCountryId);
				bdbCountryId.put(null,dbCountry,dbCountryId);
			}
		}
		catch(DatabaseException dbe)
		{	System.err.println(dbe);
			dbe.printStackTrace();
		}
		return stringCountryId;
	}
	
	public void readCountry(String sourceFile)//,String destFile) //SourceFile to read the contents from
	{
		Path pathSourceFile=Paths.get(sourceFile);
		//Path pathDestFile=Paths.get(destFile);//"graphs/movieGraphIMDb.txt");

		Pattern patternYear = Pattern.compile("[(][0-9]{4}[)]");//for matching 1st year
		Matcher matcherYear ;
		Pattern patternCountry= Pattern.compile("[A-Za-z]*$"); // for matching country from last year
		Matcher matcherCountry;
		Pattern patternEpisode=Pattern.compile("[{].*[}]");
		Matcher matcherEpisode;
		String stringCountryId="",stringCountry="";
		//String stringPrevTv="";
		String stringPrevMovie="";
		String stringPrevCountry="";
		String stringYear="";
		String []lines;
		stringFile=mapFiles.get("country");
		Cursor cursor;
		DatabaseEntry dbKey, dbValue;
		
		try
		{	bdbCountryId=new Database("graphsHash/countryId.db",null,dbConfig);
			bdbCountryMovie=new Database("graphsHash/countryMovie.db",null,dbConfig);
			bdbCountryTv=new Database("graphsHash/countryTv.db",null,dbConfig);
			bdbCountryEpisode=new Database("graphsHash/countryEpisode.db",null,dbConfig);
			bdbMovieCountry=new Database("graphsHash/movieCountry.db",null,dbConfig);
			bdbTvCountry=new Database("graphsHash/tvCountry.db",null,dbConfig);
			bdbEpisodeCountry=new Database("graphsHash/episodeCountry.db",null,dbConfig);
		}
		catch(Exception dbe)
		{	System.err.println(dbe);
			dbe.printStackTrace();
		}
		
		
		
		try
		{	reader=Files.newBufferedReader(pathSourceFile,charset);
			//writer=Files.newBufferedWriter(pathDestFile,charset);
			int matchPositionYear=0; 
			long rowsMovies=0, rowsLines=0,rowsCountry=0,rowsTv=0,rowsEpisode=0;
			while ((line = reader.readLine()) != null)
			{	rowsLines++;
				if (!(line.trim().isEmpty()))
				{	try
					{	matcherYear=patternYear.matcher(line);
						if (matcherYear.find())
						{	matchPositionYear=matcherYear.start();
								stringMovie=(line.substring(0,matchPositionYear)).trim();//matcherYear.start())).trim();
								if (stringMovie.charAt(0)!='\"')  //its a movie since " " missing
								{	rowsMovies++;
									
									matcherCountry=patternCountry.matcher(line);
									matcherCountry.find();
									stringCountry=matcherCountry.group();
									cursor=bdbMovieCountry.openCursor(null,null);
									dbKey=new DatabaseEntry(stringMovie.getBytes(charset));
									dbValue=new DatabaseEntry(stringCountry.getBytes(charset));
									
									if (cursor.getSearchBoth(dbKey,dbValue,null)!=OperationStatus.SUCCESS)
									{	
									stringMovieId=checkMovie(stringMovie,true);
									stringCountryId=checkCountry(stringCountry,true);
									updatebdbMovieCountry(stringMovie,stringMovieId,"\t"+stringYear+"\t"+stringCountry+"\t"+stringCountryId );
									updatebdbCountryMovie(stringCountry,stringCountryId,"\t"+stringMovie+"\t"+stringMovieId+"\t"+stringYear);
									}
								}
								 
/*								else  //its a TV series
								{	
									matcherCountry=patternCountry.matcher(line);
									matcherCountry.find();
									stringCountry=matcherCountry.group();
									stringCountryId=checkCountry(stringCountry,true);
									matcherEpisode=patternEpisode.matcher(line);
									try
									{	if (matcherEpisode.find()) // if episode present
										{	stringEpisode=((matcherEpisode.group()).replaceAll("\\{","")).replaceAll("\\}","");
											
											
											matcherCountry=patternCountry.matcher(line);
											matcherCountry.find();
											stringCountry=matcherCountry.group();
											
											cursor=bdbCountryEpisode.openCursor(null,null);
											dbKey=new DatabaseEntry(stringCountry.getBytes(charset));
											dbValue=new DatabaseEntry(stringEpisode.getBytes(charset));
											
											if (cursor.getSearchBoth(dbKey,dbValue,null)!=OperationStatus.SUCCESS)
											{					
											
											stringEpisodeId=checkEpisode(stringEpisode,true);
											stringCountryId=checkCountry(stringCountry,true);
											updatebdbEpisodeCountry(stringEpisode,stringEpisodeId,"\t"+stringCountry+"\t"+stringCountryId );
											updatebdbCountryEpisode(stringCountry,stringCountryId,"\t"+stringEpisode+"\t"+stringEpisodeId);
											}
											
										}
										else  //only in absence of Episode should TV-country be stored
										{	matcherCountry=patternCountry.matcher(line);
											matcherCountry.find();
											stringCountry=matcherCountry.group();
											cursor=bdbTvCountry.openCursor(null,null);
											dbKey=new DatabaseEntry(stringMovie.getBytes(charset));
											dbValue=new DatabaseEntry(stringCountry.getBytes(charset));
											
											if (cursor.getSearchBoth(dbKey,dbValue,null)!=OperationStatus.SUCCESS)
											{	
											stringCountryId=checkCountry(stringCountry,true);
											stringTvId=checkTv(stringMovie,true);
											rowsTv++;
											updatebdbTvCountry(stringMovie,stringTvId,"\t"+stringYear+"\t"+stringCountry+"\t"+stringCountryId);
											updatebdbCountryTv(stringCountry,stringCountryId,"\t"+stringMovie+"\t"+stringTvId+"\t"+stringYear);
											}
										}
									}	
									catch(IllegalStateException ise)
									{	System.err.println(ise);
										ise.printStackTrace();
									}
								/*	//if (stringTvId!="" && !(stringPrevTv.equals(stringMovie)))
									if (!(stringPrevTv.equals(stringMovie)))
									{
									}
								*/
/*									stringPrevCountry=stringCountry;
								}
						*/		
								stringPrevMovie=stringMovie;
						}
						
						//}
					}
					catch(IllegalStateException ise)
					{	System.out.println("illegal state exception"+line);
					}
				}
				//stringPrevActor=stringActor;
			}
			reader.close();
			System.out.println("finished country");
			writer=Files.newBufferedWriter(Paths.get("graphsHash/countryTv.txt"),charset);
			writeDb(bdbCountryTv,writer);
			bdbCountryTv.close();
			writer.close();			
			writer=Files.newBufferedWriter(Paths.get("graphsHash/countryMovie.txt"),charset);
			writeDb(bdbCountryMovie,writer);
			bdbCountryMovie.close();
			writer.close();			
			writer=Files.newBufferedWriter(Paths.get("graphsHash/countryEpisode.txt"),charset);
			writeDb(bdbCountryEpisode,writer);
			bdbCountryEpisode.close();
			writer.close();			
			writer=Files.newBufferedWriter(Paths.get("graphsHash/episodeCountry.txt"),charset);
			writeDb(bdbEpisodeCountry,writer);
			bdbEpisodeCountry.close();
			writer.close();			
			writer=Files.newBufferedWriter(Paths.get("graphsHash/tvCountry.txt"),charset);
			writeDb(bdbTvCountry,writer);
			bdbTvCountry.close();
			writer.close();			
			writer=Files.newBufferedWriter(Paths.get("graphsHash/movieCountry.txt"),charset);
			writeDb(bdbMovieCountry,writer);
			bdbMovieCountry.close();
			writer.close();			
//			writer=Files.newBufferedWriter(Paths.get("graphsHash/countryId.txt"),charset);
//			writeDb(bdbCountryId,writer);
			bdbCountryId.close(); //close it here and reopen an write in readlocation
//			writer.close();
		}
		
		catch (IOException e)
		{	System.err.println(e);
			e.printStackTrace();
		}		
		catch(DatabaseException dbe)
		{	System.err.println(dbe);
			dbe.printStackTrace();
		}
	}
/* ------------------end Country ---------------*/

/*-------------------begin Locations ----------------*/	

	public void updatebdbTvLoc(String stringTv, String stringTvId, String stringToAdd) // stringToAdd contains place, state, country
	{	DatabaseEntry dbKey, dbValue;
		byte[] byteKey,byteValue;
		byteKey=stringTv.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		byteValue=(stringTvId+stringToAdd).getBytes(charset);//(stringMovieId+"\t"+stringYear +"\t"+stringYearId).getBytes(charset);
		dbValue=new DatabaseEntry(byteValue);
			
		try
		{   bdbTvLoc.put(null,dbKey,dbValue); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	}

	public void updatebdbLocTv( String stringLocCountry,String stringLocCountryId,String stringLocState, String stringLocStateId, String stringLocCity,String stringLocCityId,String stringTv, String stringLocComment)
	{	DatabaseEntry dbKey,dbValue;
		byte[] byteKey,byteValue;
		byteKey=stringLocCity.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		byteValue=(stringLocCityId+"\t"+stringTv+"\t"+stringLocCountry+"\t"+stringLocCountryId+"\t"+ stringLocState + "\t"+ stringLocStateId+"\t"+stringLocComment).getBytes(charset);
		dbValue=new DatabaseEntry(byteValue);
		try
		{   bdbLocCityTv.put(null,dbKey,dbValue); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
			
		byteKey=stringLocState.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		byteValue=(stringLocStateId+"\t"+stringTv +"\t"+stringLocCountry+"\t"+stringLocCountryId+"\t"+ stringLocCity + "\t"+ stringLocCityId+"\t"+stringLocComment).getBytes(charset);
		dbValue=new DatabaseEntry(byteValue);
		try
		{   bdbLocStateMovie.put(null,dbKey,dbValue);
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
		
		byteKey=stringLocCountry.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		byteValue=(stringLocCountryId+"\t"+stringTv+"\t"+stringLocState+"\t"+stringLocStateId+"\t"+ stringLocCity+ "\t"+ stringLocCityId+"\t"+stringLocComment).getBytes(charset);
		dbValue=new DatabaseEntry(byteValue);
		try
		{	bdbLocCountryMovie.put(null,dbKey,dbValue);
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	}

	public void updatebdbMovieLoc(String stringMovie, String stringMovieId, String stringToAdd) // stringToAdd contains place, state, country
	{	DatabaseEntry dbKey, dbValue;
		byte[] byteKey,byteValue;
		byteKey=stringMovie.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		byteValue=(stringMovieId+stringToAdd).getBytes(charset);//(stringMovieId+"\t"+stringYear +"\t"+stringYearId).getBytes(charset);
		dbValue=new DatabaseEntry(byteValue);
			
		try
		{   bdbMovieLoc.put(null,dbKey,dbValue); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	}

	public void updatebdbLocMovie( String stringLocCountry,String stringLocCountryId,String stringLocState, String stringLocStateId, String stringLocCity,String stringLocCityId,String stringMovie, String stringLocComment)
	{	DatabaseEntry dbKey,dbValue;
		byte[] byteKey,byteValue;
		byteKey=stringLocCity.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		byteValue=(stringLocCityId+"\t"+stringMovie+"\t"+stringLocCountry+"\t"+stringLocCountryId+"\t"+ stringLocState + "\t"+ stringLocStateId+"\t"+stringLocComment).getBytes(charset);
		dbValue=new DatabaseEntry(byteValue);
		try
		{   bdbLocCityMovie.put(null,dbKey,dbValue); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
			
		byteKey=stringLocState.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		byteValue=(stringLocStateId+"\t"+stringMovie+"\t"+stringLocCountry+"\t"+stringLocCountryId+"\t"+ stringLocCity + "\t"+ stringLocCityId+"\t"+stringLocComment).getBytes(charset);
		dbValue=new DatabaseEntry(byteValue);
		try
		{   bdbLocStateMovie.put(null,dbKey,dbValue);
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
		
		byteKey=stringLocCountry.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		byteValue=(stringLocCountryId+"\t"+stringMovie+"\t"+stringLocState+"\t"+stringLocStateId+"\t"+ stringLocCity + "\t"+ stringLocCityId+"\t"+stringLocComment).getBytes(charset);
		dbValue=new DatabaseEntry(byteValue);
		try
		{	bdbLocCountryMovie.put(null,dbKey,dbValue);
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	}

	
	public void updatebdbEpisodeLoc(String stringEpisode, String stringEpisodeId, String stringToAdd) // stringToAdd contains place, state, country
	{	DatabaseEntry dbKey, dbValue;
		byte[] byteKey,byteValue;
		byteKey=stringEpisode.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		byteValue=(stringEpisodeId+stringToAdd).getBytes(charset);//(stringMovieId+"\t"+stringYear +"\t"+stringYearId).getBytes(charset);
		dbValue=new DatabaseEntry(byteValue);
			
		try
		{   bdbEpisodeLoc.put(null,dbKey,dbValue); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	}

	public void updatebdbLocEpisode( String stringLocCountry,String stringLocCountryId,String stringLocState, String stringLocStateId, String stringLocCity,String stringLocCityId,String stringEpisode, String stringLocComment)
	{	DatabaseEntry dbKey,dbValue;
		byte[] byteKey,byteValue;
		byteKey=stringLocCity.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		byteValue=(stringLocCityId+"\t"+stringEpisode+"\t"+stringLocCountry+"\t"+stringLocCountryId+"\t"+ stringLocState + "\t"+ stringLocStateId+"\t"+stringLocComment).getBytes(charset);
		dbValue=new DatabaseEntry(byteValue);
		try
		{   bdbLocCityEpisode.put(null,dbKey,dbValue); 
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
			
		byteKey=stringLocState.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		byteValue=(stringLocStateId+"\t"+stringEpisode+"\t"+stringLocCountry+"\t"+stringLocCountryId+"\t"+ stringLocCity + "\t"+ stringLocCityId+"\t"+stringLocComment).getBytes(charset);
		dbValue=new DatabaseEntry(byteValue);
		try
		{   bdbLocStateEpisode.put(null,dbKey,dbValue);
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
		
		byteKey=stringLocCountry.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		byteValue=(stringLocCountryId+"\t"+stringEpisode+"\t"+stringLocState+"\t"+stringLocStateId+"\t"+ stringLocCity + "\t"+ stringLocCityId+"\t"+stringLocComment).getBytes(charset);
		dbValue=new DatabaseEntry(byteValue);
		try
		{	bdbLocCountryEpisode.put(null,dbKey,dbValue);
		}
		catch(DatabaseException dbe){System.err.println(dbe);}
	}

	public String checkLocCity(String stringLoc,Boolean booleanAdd)
	{	DatabaseEntry dbKey, dbValue;
		byte[] byteKey,byteValue;
		//String stringCountryIMDb;
		byteKey=stringLoc.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);	
		dbValue=new DatabaseEntry();
		String stringValueId="";
		try
		{
			if(bdbLocCityId.exists(null,dbKey)==OperationStatus.SUCCESS)
			{	if(bdbLocCityId.get(null,dbKey,dbValue,null)==OperationStatus.SUCCESS)
				{	stringValueId=new String(dbValue.getData(),charset);
					//return stringActorId;
				}
			}
			else if (booleanAdd)
			{	longLocCityId++;
				stringValueId=stringCityFile+Long.toString(longLocCityId);
				byteValue=stringValueId.getBytes(charset);//(Long.toString(yearId)).getBytes();
				dbValue=new DatabaseEntry(byteValue);
				bdbLocCityId.put(null,dbKey,dbValue);
			}
		}
		catch(DatabaseException dbe)
		{	System.err.println(dbe);
			dbe.printStackTrace();
		}
		return stringValueId;
	}
	
	public String checkLocState(String stringLoc,Boolean booleanAdd)
	{	DatabaseEntry dbKey, dbValue;
		byte[] byteKey,byteValue;
		//String stringCountryIMDb;
		byteKey=stringLoc.getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);	
		dbValue=new DatabaseEntry();
		String stringValueId="";
		try
		{
			if(bdbLocStateId.exists(null,dbKey)==OperationStatus.SUCCESS)
			{	if(bdbLocStateId.get(null,dbKey,dbValue,null)==OperationStatus.SUCCESS)
				{	stringValueId=new String(dbValue.getData(),charset);
					//return stringActorId;
				}
			}
			else if (booleanAdd)
			{	longLocStateId++;
				stringValueId=stringStateFile+Long.toString(longLocStateId);
				byteValue=stringValueId.getBytes(charset);
				dbValue=new DatabaseEntry(byteValue);
				bdbLocStateId.put(null,dbKey,dbValue);
			}
		}
		catch(DatabaseException dbe)
		{	System.err.println(dbe);
			dbe.printStackTrace();
		}
		return stringValueId;
	}


	public void readLocation(String sourceFile)//,String destFile) //SourceFile to read the contents from
	{
		Path pathSourceFile=Paths.get(sourceFile);

		Pattern patternYear=Pattern.compile("[(][0-9?]{4}.*[)]");//to cover {????}, {1942/I) ...
		Matcher matcherYear;
		Pattern patternEpisode=Pattern.compile("[{].*[}]");
		Matcher matcherEpisode;
		
		String stringLocCity, stringLocState, stringLocCountry;
		String stringLocCityId, stringLocStateId, stringLocCountryId;
		String stringYear, stringYearId;
		
		String stringEpisode, stringEpisodeId, stringMovie, stringMovieId, stringTvId;
		String stringPrevTv="";
		String stringPrevPlace="";
		String []stringTemp;
		String []lines;
		String []stringLine;
		String stringLocComment;
		stringCityFile=mapFiles.get("LocCity");
		stringFile=mapFiles.get("country"); //using the same countryId.db
		stringStateFile=mapFiles.get("locState");
		int intL; //for length of array
		longLocCountryId=0;
		longLocCityId=0;
		longLocStateId=0;
		try
		{
			bdbLocCityId=new Database("graphsHash/locCityId.db",null,dbConfig);
			bdbLocStateId=new Database("graphsHash/locStateId.db",null,dbConfig);
//			bdbLocCountryId=new Database("graphsHash/locCountryId.db",null,dbConfig);//using countryId.db
			if (bdbCountryId==null) bdbCountryId=new Database("graphsHash/countryId.db",null,dbConfig);//reusing countryId.db
		
			bdbLocCountryEpisode=new Database("graphsHash/locCountryEpisode.db",null,dbConfig);
			bdbLocStateEpisode=new Database("graphsHash/locStateEpisode.db",null,dbConfig);
			bdbLocCityEpisode=new Database("graphsHash/LocCityEpisode.db",null,dbConfig);
			bdbEpisodeLoc=new Database("graphsHash/episodeLoc.db",null,dbConfig);
			
			bdbLocCountryTv=new Database("graphsHash/locCountryTv.db",null,dbConfig);
			bdbLocStateTv=new Database("graphsHash/locStateTv.db",null,dbConfig);
			bdbLocCityTv=new Database("graphsHash/locCityTv.db",null,dbConfig);
			bdbTvLoc=new Database("graphsHash/tvLoc.db",null,dbConfig);
	
			bdbLocCountryMovie=new Database("graphsHash/locCountryMovie.db",null,dbConfig);
			bdbLocStateMovie=new Database("graphsHash/locStateMovie.db",null,dbConfig);
			bdbLocCityMovie=new Database("graphsHash/locCityMovie.db",null,dbConfig);
			bdbMovieLoc=new Database("graphsHash/movieLoc.db",null,dbConfig);
		}
		catch(Exception dbe)
		{	System.err.println(dbe);
			dbe.printStackTrace();
		}
		
		
		
		try
		{	reader=Files.newBufferedReader(pathSourceFile,charset);
			//writer=Files.newBufferedWriter(pathDestFile,charset);
			int matchPositionYear=0; 
			long rowsMovies=0, rowsLines=0,rowsCountry=0,rowsTv=0,rowsEpisode=0;
			while ((line = reader.readLine()) != null)
			{	rowsLines++;
				if (!(line.trim().isEmpty()))
				{	try
					{	stringLine=line.split("\t");
						stringMovie=stringLine[0]; 
						stringLocComment="";
						stringYear="";
						//System.out.println(line);
						//if (stringMovie.charAt(0)=='\"')  //its a TV series
						//{//	stringTvId=checkTv(stringMovie,true);
							try
							{	matcherYear=patternYear.matcher(stringLine[0]);
								if (matcherYear.find())
								{	stringYear=matcherYear.group();
									stringYearId=checkYear(stringYear,true);
									stringMovie=(stringLine[0].substring(0,matcherYear.start())).trim();
								//	System.out.println("stringYear:" + stringYear);
								}
								else
								{	stringYear="";
									stringMovie="";
								}
							}
							catch(IllegalStateException ise)
							{	ise.printStackTrace();
							}
							
							matcherEpisode=patternEpisode.matcher(stringLine[0]);
							
							
							intL=stringLine.length-1; // can be of 2 or 3 elements; last element can be location comments
							
							stringLocComment=stringLine[intL];
							if (stringLocComment.charAt(0)=='(') //its a location comment
								intL--;
							else
								stringLocComment="";
							stringTemp=stringLine[intL].split(",");
							intL=stringTemp.length-1;
							if (intL>=0)
								stringLocCountry=stringTemp[intL--];
							else	
								stringLocCountry="";
							if (intL>=0)
								stringLocState=stringTemp[intL--];
							else
								stringLocState="";
							if (intL>=0)
								stringLocCity=stringTemp[intL--];
							else
								stringLocCity="";
							for(;intL>=0;--intL) //iteratively adding the remaining places to comment, generatlly it is only 1 place. Did not generate an ID
									stringLocComment=stringTemp[intL]+"\t"+	stringLocComment;
							
							stringLocCityId=checkLocCity(stringLocCity,true);
							stringLocStateId=checkLocState(stringLocState,true);
							stringLocCountryId=checkCountry(stringLocCountry,true);
							//System.out.println(stringMovie+" stringmovie");
							try
							{if (stringMovie.charAt(0)=='\"')  //its a TV series
							{	try
								{
									if (matcherEpisode.find())
									{	stringEpisode=matcherEpisode.group();
										stringEpisodeId=checkEpisode(stringEpisode,true);
									
										updatebdbLocEpisode(stringLocCountry, stringLocCountryId, stringLocState, stringLocStateId, stringLocCity,stringLocCityId,stringEpisode+"\t"+stringEpisodeId+"\t"+stringYear,stringLocComment);
										updatebdbEpisodeLoc(stringEpisode,stringEpisodeId, stringLocCountry+"\t"+stringLocCountryId+"\t"+stringLocState+
											"\t"+stringLocStateId + stringLocCity+stringLocCityId+"\t"+stringLocComment);
									}
									stringTvId=checkTv(stringMovie,true);
									if (stringTvId.length()>0)
									{	updatebdbLocTv(stringLocCountry, stringLocCountryId, stringLocState, stringLocStateId, stringLocCity,stringLocCityId,stringTv+"\t"+stringTvId+"\t"+stringYear,stringLocComment);
										updatebdbTvLoc(stringMovie,stringTvId, stringYear+"\t"+stringLocCountry+"\t"+stringLocCountryId+"\t"+stringLocState+
												"\t"+stringLocStateId + stringLocCity+stringLocCityId+"\t"+stringLocComment);
									}	
									
								}
								catch(IllegalStateException ise)
								{	ise.printStackTrace();
								}
							}
							else
							{   stringMovieId=checkMovie(stringMovie,true);
								updatebdbLocMovie(stringLocCountry, stringLocCountryId, stringLocState, stringLocStateId, stringLocCity,stringLocCityId,stringMovie+"\t"+ stringMovieId+"\t"+stringYear,stringLocComment);
								updatebdbMovieLoc(stringMovie,stringMovieId, stringYear+"\t"+stringLocCountry+"\t"+stringLocCountryId+"\t"+stringLocState+
												"\t"+stringLocStateId + stringLocCity+stringLocCityId+"\t"+stringLocComment);
							}	
							}catch(StringIndexOutOfBoundsException sibe)
							{	System.out.println(line);
								System.out.println(stringMovie);
							}
					}
					catch(IllegalStateException ise)
					{	System.out.println("illegal state exception"+line);
					}
				}
			}
			reader.close();
//			writer=Files.newBufferedWriter(Paths.get("graphsHash/locCountryId.txt"),charset);
//			writeDb(bdbLocCountryId,writer);
//			bdbLocCountryId.close();
//			writer.close();

			writer=Files.newBufferedWriter(Paths.get("graphsHash/locStateId.txt"),charset);
			writeDb(bdbLocStateId,writer);
			bdbLocStateId.close();
			writer.close();			
			writer=Files.newBufferedWriter(Paths.get("graphsHash/locCityId.txt"),charset);
			writeDb(bdbLocCityId,writer);
			bdbLocCityId.close();
			writer.close();			
			writer=Files.newBufferedWriter(Paths.get("graphsHash/locCountryEpisode.txt"),charset);
			writeDb(bdbLocCountryEpisode,writer);
			bdbLocCountryEpisode.close();
			writer.close();			
			writer=Files.newBufferedWriter(Paths.get("graphsHash/locStateEpisode.txt"),charset);
			writeDb(bdbLocStateEpisode,writer);
			bdbLocStateEpisode.close();
			writer.close();			
			writer=Files.newBufferedWriter(Paths.get("graphsHash/locCityEpisode.txt"),charset);
			writeDb(bdbLocCityEpisode,writer);
			bdbLocCityEpisode.close();
			writer.close();			
			writer=Files.newBufferedWriter(Paths.get("graphsHash/episodeLoc.txt"),charset);
			writeDb(bdbEpisodeLoc,writer);
			bdbEpisodeLoc.close();
			writer.close();	

			writer=Files.newBufferedWriter(Paths.get("graphsHash/tvLoc.txt"),charset);
			writeDb(bdbTvLoc,writer);
			bdbTvLoc.close();
			writer.close();
			writer=Files.newBufferedWriter(Paths.get("graphsHash/locCountryTv.txt"),charset);
			writeDb(bdbLocCountryTv,writer);
			bdbLocCountryTv.close();
			writer.close();
			writer=Files.newBufferedWriter(Paths.get("graphsHash/locStateTv.txt"),charset);
			writeDb(bdbLocStateTv,writer);
			bdbLocStateTv.close();
			writer.close();
			writer=Files.newBufferedWriter(Paths.get("graphsHash/locCityTv.txt"),charset);
			writeDb(bdbLocCityTv,writer);
			bdbLocCityTv.close();
			writer.close();
			
			writer=Files.newBufferedWriter(Paths.get("graphsHash/movieLoc.txt"),charset);
			writeDb(bdbMovieLoc,writer);
			bdbMovieLoc.close();
			writer.close();
			writer=Files.newBufferedWriter(Paths.get("graphsHash/locCountryMovie.txt"),charset);
			writeDb(bdbLocCountryMovie,writer);
			bdbLocCountryMovie.close();
			writer.close();
			writer=Files.newBufferedWriter(Paths.get("graphsHash/locStateMovie.txt"),charset);
			writeDb(bdbLocStateMovie,writer);
			bdbLocStateMovie.close();
			writer.close();
			writer=Files.newBufferedWriter(Paths.get("graphsHash/locCityMovie.txt"),charset);
			writeDb(bdbLocCityMovie,writer);
			bdbLocCityMovie.close();
			writer.close();	
		}
		
		catch (IOException e)
		{	System.err.println(e);
			e.printStackTrace();
		}		
		catch(DatabaseException dbe)
		{	System.err.println(dbe);
			dbe.printStackTrace();
		}
	}

/*----------------------end locations---------------------------------*/	
	
	void printId()
	{	System.out.println("Movies:"+longMovieId + "\nTv:"+longTvId+"\nEpisode"+longEpisodeId+"\nYear:"+longYearId+
			"\nActor:"+longActorId+"\nActress:"+longActressId+"\nProducer:"+longProducerId+"\nDirector:"+longDirectorId+
			"\nCountry:"+longCountryId+"\nLanguage:"+longLanguageId+"\nGenre:"+longGenreId);
	}

	void writeDb(Database bdbTemp, BufferedWriter writerTemp)		
	{		
		dbPerson=new DatabaseEntry();
		dbPersonValue=new DatabaseEntry();
		String stringPerson="";
		
		try
		{
			Cursor cursorCursor=bdbTemp.openCursor(null,null);
			while (cursorCursor.getPrev(dbPerson,dbPersonValue,LockMode.DEFAULT)==OperationStatus.SUCCESS)
			{	try
				{	stringPerson=new String(dbPerson.getData(),charset);
				}catch(NullPointerException npe)
				{	stringPerson="";
				}
				try
				{	stringPersonValue = new String(dbPersonValue.getData(),charset);
				}
				catch(NullPointerException npe)
				{	npe.printStackTrace();
				}
				stringPersonValue=stringPerson+"\t"+stringPersonValue+ "\n";
				dbPerson=new DatabaseEntry(); // need to refresh this, otherwise previous leftover remains
				dbPersonValue=new DatabaseEntry();

				try
				{//	System.out.println("stringPersonValue"+stringPersonValue);
					writerTemp.write(stringPersonValue,0,stringPersonValue.length());
				}
				catch (IOException ioe)
				{	System.err.println(ioe);
					ioe.printStackTrace();
				}
			}
		}

		catch(DatabaseException dbe)
		{	System.err.println(dbe);
			dbe.printStackTrace();
		}
		
	}
	
	void callWriteDb() // calls writeDb for leftover bdb
	{	try
		{
			writer=Files.newBufferedWriter(Paths.get("graphsHash/movieId.txt"),charset);
			writeDb(bdbMovieId,writer);
			bdbMovieId.close();
			writer.close();
			writer=Files.newBufferedWriter(Paths.get("graphsHash/episodeId.txt"),charset);
			writeDb(bdbEpisodeId,writer);
			bdbEpisodeId.close();
			writer.close();
			writer=Files.newBufferedWriter(Paths.get("graphsHash/tvId.txt"),charset);
			writeDb(bdbTvId,writer);
			bdbTvId.close();
			writer.close();
			writer=Files.newBufferedWriter(Paths.get("graphsHash/yearId.txt"),charset);
			writeDb(bdbYearId,writer);
			writer.close();
			bdbYearId.close();
			writer=Files.newBufferedWriter(Paths.get("graphsHash/episodeTv.txt"),charset);
			writeDb(bdbEpisodeTv,writer);
			bdbEpisodeTv.close();
			writer.close();
			writer=Files.newBufferedWriter(Paths.get("graphsHash/tvEpisode.txt"),charset);
			writeDb(bdbTvEpisode,writer);
			bdbTvEpisode.close();
			writer.close();
			writer=Files.newBufferedWriter(Paths.get("graphsHash/person.txt"),charset);
			writeDb(bdbPerson,writer);
			bdbPerson.close();
			writer.close();
			writer=Files.newBufferedWriter(Paths.get("graphsHash/personId.txt"),charset);
			writeDb(bdbPersonId,writer);
			bdbPersonId.close();
			writer.close();
		}
		catch(IOException ioe)
		{	System.err.println(ioe);
			ioe.printStackTrace();
		}
		catch(DatabaseException dbe)
		{	System.err.println(dbe);
		}
	}

	void printKeyOnly()
	{	Cursor cursor;
		DatabaseEntry dbKey, dbData;
		byte[] byteKey, byteData;
		byteKey="1937".getBytes(charset);
		dbKey=new DatabaseEntry(byteKey);
		dbData=new DatabaseEntry();
		try
		{	cursor=bdbYearEpisode.openCursor(null,null);
			if (cursor.getSearchKey(dbKey,dbData,null)==OperationStatus.SUCCESS)
			{	int c=cursor.count();
				System.out.println("count for 1937 in bdbYearEpisode"+cursor.count());
				do 
				{	System.out.println(c+" " +new String(dbKey.getData(),charset)+": "+ new String(dbData.getData(),charset));
					dbData=new DatabaseEntry();
					c--;
				}while	(cursor.getNext(dbKey,dbData,null)==OperationStatus.SUCCESS && c>0);
					
				
			}
		}
		catch(DatabaseException dbe)
		{	System.err.println(dbe);
			dbe.printStackTrace();
		}
	
	}
	public static void main(String[] args)
	{	
		System.out.println("start movies:"+ (new Date()));
		graphIMDbComplete21 myGraph=new graphIMDbComplete21();
	
/*		myGraph.readMovies("IMDbData/movies.list");
		System.out.println("start actorsEpisode "+(new Date()));	
		myGraph.readActorsEpisode("IMDbData/actors.list");
		System.out.println("start actorsMovie"+(new Date()));	
		myGraph.readActorsMovie("IMDbData/actors.list");
		System.out.println("start actressEpisode "+(new Date()));
		myGraph.readActressEpisode("IMDbData/actresses.list");
		System.out.println("start actressMovie "+(new Date()));
		myGraph.readActressMovie("IMDbData/actresses.list");
	
		System.out.println("start directors "+(new Date()));
		myGraph.readDirector("IMDbData/directors.list");
		System.out.println("start producers "+(new Date()));
		myGraph.readProducer("IMDbData/producers.list");
		System.out.println("start genres "+(new Date()));
		myGraph.readGenre("IMDbData/genres.list");
		System.out.println("start language "+(new Date()));
		myGraph.readLanguage("IMDbData/language.list");
*/		System.out.println("start countries "+(new Date()));
		myGraph.readCountry("IMDbData/countries.list");
//		myGraph.readLocation("IMDbData/locations.list");
		System.out.println("end time:"+(new Date()));
		myGraph.callWriteDb();
		myGraph.printId();
	
		
	}
				
}
	

