## Assignment 2 by Yonah Solomon & Ohad Heller ##

## Personal info:
## Yonah Solomon:
    ID: 320440639
    UserName:yonahs
## Ohad Heller:
    ID: 318360997
    UserName: ohadhell

## Running instructions:

1. Setup AWS credentials:
    1.1 Run: "aws configure" in the terminal and enter your aws_access_key_id and aws_secret_access_key
    1.2 Run: "aws configure set aws_session_token "<<your session token>>""  ##Note the "" are required.
2. Open the terminal in the Assignment directory 
3. Run this commands:
    cd xmleRunner && mvn package && java -jar target/main-jar-with-dependencies
4. Application started running. You may enter EMR in AWS console and verify the cluster started.
5. Results will be under 

## Workflow by jobs (Numbered like mleManager.java):

1. paratiotions And N - Divides the curpos to P0 P1 and calculates N.
2. calculate Nr and Tr for each partition and give each trigram it's Nr0, Nr1, Tr01, Tr10.
3. for each trigram sum its Nr and Tr.
4. calculate probabilities for each trigram.
5. sort results as instructed.

## Results on Google ngram ("3n://datasets.elasticmapreduce/ngrams/books/20090715/eng-fiction-all/3gram/data")

Sample result file found is zip archive (Results.txt)
Complete results (and other steps outputs as well) found in: s3://ohad-and-yonah-done-bucket/output/1672750829290/

## Statistics Analysis:
We chose 10 'interesting' word pairs (W1 W2), and for each we'll show their top 5 next words (W3).
For each of the 10 we'll judge if the system got to a reasonable result.

1. AND WHAT:
        TO	1.0979264806703417E-6	
        THEY	8.828674533116355E-7	
        HE	8.063964029920311E-7	
        ARE	3.1733511013054456E-7	
        ABOUT	1.6895721037327177E-7
2. East African:
        territories	1.2006389089831832E-6	
        slave	3.791097018716759E-7	
        Studies	3.309006799676438E-7	
        Mission	2.1176980748895608E-7	
        territory	1.9426751656792206E-7
3. He turned:
        aside	7.84063239679684E-6	
        white	1.860610625906545E-6	
        just	1.4534135073874529E-6	
        briskly	4.910941140407376E-7	
        quietly	4.041093357976578E-7
4. Her voice:
        cracked	5.819906664289041E-6	
        changed	2.7788174507166123E-6	
        drifted	1.949374500652891E-6	
        trails	1.1379826522714145E-6	
        tightened	4.536465954167645E-7
5. NEW YORK:
        SOCIETY	3.309006799676438E-7	
        DUFFIELD	3.153443506127444E-7	
        COLLIER	2.8138507007251625E-7	
        FOR	2.7283323887985027E-7	
        MEDICAL	2.600228345627448E-7	
6. National Security:
        Act	9.528405329440662E-6	
        Policy	2.3902221323568476E-6	
        State	1.4119639542254254E-6	
        Resources	1.4045483649434367E-6	
        Service	4.2396020815218737E-7
7. American soldiers:
        fought	2.2142417489290744E-7	
        did	2.0538112058354552E-7	
        captured	9.509654624651833E-8	
        coming	8.596127057115531E-8	
        like	5.5796791224595136E-8
8. Jewish religious:
        practices	6.00831622921515E-7	
        philosophy	1.6987628526482496E-7	
        teaching	9.509654624651833E-8	
        authority	8.439723211408105E-8	
        teachings	6.703238710471248E-8
9. health insurance:
        benefits	2.1380442231265375E-6	
        industry	1.0219071407589874E-6	
        contract	1.237715009051641E-7	
        covered	1.1324053908629107E-7	
        provisions	7.134918788585387E-8
10. heard somebody:
        moving	7.538383414169736E-7	
        running	4.308237376273147E-7	
        speaking	1.1262591820246587E-7	
        inside	1.0393698461531272E-7	
        snoring	8.903863652172601E-8

record -
bytes - 

without - 
    split - 
        176_390_523
        817_114_928

    calc Nr0 -
        4_277_236
        3_824_516

    calc Tr0 - 
        4_277_236
        14_307_908

    sum Nr for p0
        4_284_106
        91_353_859
    
    sum Tr for p0 -
        4_284_106
        91_380_813
    
    calc Nr1 - 
        4_277_236
        3_824_751

    calc Tr0
        4_277_236
        14_311_820
    
    sum Nr for p1
        4_284_053
        91_353_538

    sum Tr for p1
        4_284_053
        91_380_263

    sum Nr total 
        8_554_472
        150_951_619
    
    sum Tr total 
        8_554_472
        152_968_925
    
    calc prob 
        8_554_472
        149_063_184
    
    sort
        4_272_167
        127_331_281




with -

     split - 
        8_554_516
        128_563_370
        

    calc Nr0 -
        34_920
        234_153
        

    calc Tr0 - 
        34_920
        320_640
        

    sum Nr for p0
        4_284_050
        91_357_847
        
    
    sum Tr for p0 -
        4_284_050
        91_384_440
        
    
    calc Nr1 - 
        34_991
        234_414


    calc Tr0
        34_991
        321_177
        
    
    sum Nr for p1
        4_284_116
        91_358_034

    sum Tr for p1
        4_284_116
        91_385_014
        

    sum Nr total
        8_554_472
        150_920_482 
        
    
    sum Tr total 
        8_554_472
        152_969_832
        
    
    calc prob 
        8_554_472
        149_016_943
        
    
    sort
        4_272_213
        127_430_899