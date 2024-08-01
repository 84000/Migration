// Importing necessary libraries
// @ts-ignore
import {serve} from "https://deno.land/std@0.168.0/http/server.ts"
// @ts-ignore
import {createClient} from 'https://esm.sh/@supabase/supabase-js@2'
// @ts-ignore

// Supabase credentials
const url = "https://ivwvvjgudwqwjbclvfjy.supabase.co";
const key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Iml2d3Z2amd1ZHdxd2piY2x2Zmp5Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTcwNzUwMTY3NCwiZXhwIjoyMDIzMDc3Njc0fQ.fB00yuZqZkRt-vdduxZ5BVuUgNYBxMk4mdwu-k3ERgE";
const supabase = createClient(url, key);



// Utility functions to handle data transformation
function makeTitle(work: any, workId: string) {
  const titles = work['work'][0]['title'];
  return titles.map((title: any) => ({
    work_uuid: workId,
    type: title['titleType'],
    content: title['content'],
    language: title['language'],
    migrationId: title['titleMigrationId'],
  }));
}

function makePassages(work: any, workId: string) {
  if (!('translation' in work)) return null;
  const passages = work['translation']['passage'];
  return passages.map((passage: any) => ({
    work_uuid: workId,
    xmlId: passage['xmlId'],
    parent: passage['parentId'],
    label: passage['passageLabel'],
    type: passage['segmentationType'],
    sort: passage['passageSort'],
    content: passage['content'],
  }));
}

async function getTohID(xmlID) {
  const url = 'https://read.84000-translate.org/translations.json?api-version=0.4.0';
  try {
    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    const works = await response.json();

    // Ensure the data structure is as expected
    if (works && Array.isArray(works.work)) {
      // find toh by xmlID
      const tohID = works.work.find(work => work['workId'] === xmlID)['catalogueWorkIds'];
      console.log('📥 Get tohID:', tohID);
      return tohID;
    } else {
      throw new Error('Unexpected data structure');
    }
  } catch (error) {
    console.error('Error fetching translations:', error);
  }
}

async function makeWork(translation: any) {
 const currentWork = translation['work'][0];
 const tohID =  await getTohID(currentWork['workId']);
  return {
    xmlId: currentWork['workId'],
    url: currentWork['url'],
    type: currentWork['workType'],
    toh: tohID,
    publicationDate: new Date(translation['publicationDate']).toISOString().split('T')[0],
    publicationStatus: translation['publicationStatus'],
    publicationVersion: translation['publicationVersion'],
    title: currentWork['title'][0]['content'],
    migrationJson: translation,
  };
}

// Supabase data operations
async function storeWork(workDict: any) {
  console.log("☁️ Storing Work", workDict.xmlId);
  try {
    // Check if the work already exists with the same xmlId
    const { data: existingWork, error: selectError } = await supabase
        .from('works')
        .select('uuid')
        .eq('xmlId', workDict.xmlId);

    console.log("Existing Work", existingWork);

    if (selectError) {
      throw selectError as Error;
    }

    if (existingWork && existingWork.length > 0) {
      console.log("⚠️ Work Already Exists, Delete Work and its Titles and Passages");
      // Delete the work and all its titles and passages
      const { error: deleteError } = await supabase
          .from('works')
          .delete()
          .eq('uuid', existingWork[0].uuid);

      if (deleteError) {
        console.error("Error Deleting Work", deleteError);
        throw deleteError;
      }
      console.log("✅ Deleted Work and its Titles and Passages");
    }

    console.log("Inserting Work",workDict.xmlId);

    // Insert the work and return the workId
    const { data: InsertedworkData, error: insertError } = await supabase
        .from('works')
        .upsert(workDict).select();
    console.log("Work Data", InsertedworkData);

    if (insertError) {
        console.error("Error Inserting Work", insertError);
      throw insertError;
    }
  console.log("✅ Added Work", InsertedworkData[0].uuid);
    return InsertedworkData[0].uuid;
  } catch (error) {
    console.error("Error Occurred in work", error);
    return null;
  }
}

async function storeTitles(titlesDicts: any) {
  try {
    const { data: titleData, error } = await supabase
        .from('titles')
        .upsert(titlesDicts).select();

    if (error) throw error;

    console.log("✅ Added titles", titleData.length);
    return "success";
  } catch (error) {
    console.error("Error Occurred:", error);
    return error.message;
  }
}

async function storePassages(passageDict: any) {
  try {
    const { data: passageData, error } = await supabase
        .from('passages')
        .upsert(passageDict).select();

    if (error) throw error;

    console.log("✅ Added passages", passageData.length);
    return "success";
  } catch (error) {
    console.error("Error Occurred:", error);
    return error.message;
  }
}

// Main function to handle incoming requests
async function handleRequest(request: Request) {
  const jsonPayload = await request.json();
  const translation = JSON.parse(jsonPayload.content);
  console.log("📥 Received Translation", translation['work'][0]['workId']);
  const workDict = await makeWork(translation);
  try {
    const workUuid = await storeWork(workDict);
    console.log("New Work UUID", workUuid);
    if (workUuid) {
      console.log("Work UUID", workUuid);

      console.log("Loading Titles");
      const titlesDicts = makeTitle(translation, workUuid);
      console.log("Loading Passages");
      const passageDict = makePassages(translation, workUuid);

      console.log("☁️ Storing Title");
      const returnTitles = await storeTitles(titlesDicts);
      if (returnTitles !== "success") throw new Error(returnTitles);

      if (passageDict) {
        console.log("Storing Passages");
        const returnPassages = await storePassages(passageDict);
        if (returnPassages !== "success") throw new Error(returnPassages);
      } else {
        console.log("❌ No Passages to Store");
      }
    }
    console.log("✅ Addition Complete", workDict.xmlId)
  } catch (error) {
    console.error('‼️ Error:', error.message);
    await supabase.from('Error').upsert({
      error: error.message,
      xmlId: workDict.xmlId,
    }).select();
  }

  return new Response("Processed", { status: 200   });
}

// Start the server
serve(handleRequest);